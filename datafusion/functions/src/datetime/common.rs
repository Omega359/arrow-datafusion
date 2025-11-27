// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, GenericStringArray, PrimitiveArray,
    StringArrayType, StringViewArray,
};
use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use arrow_buffer::ArrowNativeType;
use chrono::format::{parse, Parsed, StrftimeItems};
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_datafusion_err, unwrap_or_internal_err,
    DataFusionError, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use jiff::civil::{DateTime, Time};
use jiff::fmt::temporal::DateTimeParser;
use jiff::tz::{Disambiguation, TimeZone};
use jiff::{Timestamp, Zoned};
use num_traits::{PrimInt, ToPrimitive};

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

#[allow(unused)]
/// Calls string_to_timestamp_nanos and converts the error type
pub(crate) fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

pub(crate) fn string_to_timestamp_nanos_with_timezone(
    timezone: &Option<TimeZone>,
    s: &str,
) -> Result<i64> {
    let tz = timezone.to_owned().unwrap_or(TimeZone::UTC);
    let dt = string_to_datetime(&tz, s)?;
    let parsed = dt
        .timestamp()
        .as_nanosecond()
        .to_i64()
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))?;

    Ok(parsed)
}

/// Checks that all the arguments from the second are of type [Utf8], [LargeUtf8] or [Utf8View]
///
/// [Utf8]: DataType::Utf8
/// [LargeUtf8]: DataType::LargeUtf8
/// [Utf8View]: DataType::Utf8View
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

static PARSER: DateTimeParser = DateTimeParser::new();

/// Accepts a string and parses it relative to the provided `timezone`
///
/// In addition to RFC3339 / ISO8601 standard timestamps, it also
/// accepts strings that use a space ` ` to separate the date and time
/// as well as strings that have no explicit timezone offset.
///
/// Examples of accepted inputs:
/// * `1997-01-31T09:26:56.123Z`        # RCF3339
/// * `1997-01-31T09:26:56.123-05:00`   # RCF3339
/// * `1997-01-31 09:26:56.123-05:00`   # close to RCF3339 but with a space rather than T
/// * `2023-01-01 04:05:06.789 -08`     # close to RCF3339, no fractional seconds or time separator
/// * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
/// * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
/// * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
/// * `1997-01-31 092656`               # close to RCF3339, no fractional seconds
/// * `1997-01-31 092656+04:00`         # close to RCF3339, no fractional seconds or time separator
/// * `1997-01-31`                      # close to RCF3339, only date no time
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// Some formats supported by PostgresSql <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TIME-TABLE>
/// are not supported, like
///
/// * "2023-01-01 04:05:06.789 +07:30:00",
/// * "2023-01-01 040506 +07:30:00",
/// * "2023-01-01 04:05:06.789 PST",
///
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime(
    timezone: &TimeZone,
    s: &str,
) -> std::result::Result<Zoned, ArrowError> {
    let err = |ctx: &str| {
        ArrowError::ParseError(format!("Error parsing timestamp from '{s}': {ctx}"))
    };

    if s.len() < 10 {
        return Err(err("timestamp must contain at least 10 characters"));
    }

    let result = PARSER.parse_timestamp(s);
    match result {
        Ok(r) => Ok(r.to_zoned(timezone.to_owned())),
        Err(_) => {
            let datetime = if s.len() == 10 {
                let date = PARSER
                    .parse_date(s)
                    .map_err(|_| err("error parsing date"))?;
                date.to_datetime(Time::midnight())
            } else {
                PARSER
                    .parse_datetime(s)
                    .map_err(|_| err("error parsing timestamp"))?
            };

            timezone
                .to_owned()
                .into_ambiguous_zoned(datetime)
                .disambiguate(Disambiguation::Compatible)
                .map_err(|_| err("error computing timezone offset"))
        }
    }
}

/// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
/// relative to the provided `timezone`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// Note that parsing [IANA timezones] is not supported yet in chrono - <https://github.com/chronotope/chrono/issues/38>
/// and this implementation only supports named timezones at the end of the string preceded by a space.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime_formatted(
    timezone: &TimeZone,
    s: &str,
    format: &str,
) -> Result<Zoned, DataFusionError> {
    let err = |err_ctx: &str| {
        exec_datafusion_err!(
            "Error parsing timestamp from '{s}' using format '{format}': {err_ctx}"
        )
    };

    let format = if format.contains("%+") {
        "%Y-%m-%dT%H:%M:%S%.f%:z"
    } else {
        format
    };

    if format == "%c" {
        // switch to chrono, jiff doesn't support parsing with %c
        let mut parsed = Parsed::new();
        parse(&mut parsed, s, StrftimeItems::new(format))
            .map_err(|e| err(&e.to_string()))?;
        let offset = timezone
            .to_fixed_offset()
            .map_err(|e| err(&e.to_string()))?
            .to_string();
        let tz = timezone.iana_name().unwrap_or(&offset);
        let tz: Tz = tz.parse::<Tz>().ok().unwrap_or("UTC".parse::<Tz>()?);

        return match parsed.to_datetime_with_timezone(&tz) {
            Ok(dt) => {
                let dt = dt.fixed_offset();
                Ok(Timestamp::from_nanosecond(dt.timestamp() as i128)
                    .map_err(|e| err(&e.to_string()))?
                    .to_zoned(timezone.to_owned()))
            }
            Err(e) => Err(err(&e.to_string())),
        };
    }

    let result = Zoned::strptime(format, s);

    match result {
        Ok(z) => Ok(z),
        Err(_) => {
            let result = DateTime::strptime(format, s)
                .map_err(|e| err(&format!("error parsing timestamp: {e:?}")));
            let datetime = match result {
                Ok(d) => d,
                Err(e) => return Err(e),
            };

            timezone
                .to_owned()
                .into_ambiguous_zoned(datetime)
                .disambiguate(Disambiguation::Compatible)
                .map_err(|_| err("error computing timezone offset"))
        }
    }
}

/// Accepts a string with a `chrono` format and converts it to a
/// nanosecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
#[inline]
#[allow(unused)]
pub(crate) fn string_to_timestamp_nanos_formatted(
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    string_to_datetime_formatted(&TimeZone::UTC, s, format)?
        .timestamp()
        .as_nanosecond()
        .to_i64()
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))
}

pub(crate) fn string_to_timestamp_nanos_formatted_with_timezone(
    timezone: &Option<TimeZone>,
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    let dt = string_to_datetime_formatted(
        &timezone.to_owned().unwrap_or(TimeZone::UTC),
        s,
        format,
    )?;
    let parsed = dt
        .timestamp()
        .as_nanosecond()
        .to_i64()
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))?;

    Ok(parsed)
}

/// Accepts a string with a `chrono` format and converts it to a
/// millisecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
#[inline]
pub(crate) fn string_to_timestamp_millis_formatted(s: &str, format: &str) -> Result<i64> {
    Ok(string_to_datetime_formatted(&TimeZone::UTC, s, format)?
        .timestamp()
        .as_millisecond())
}

pub(crate) struct ScalarDataType<T: PrimInt> {
    data_type: DataType,
    _marker: PhantomData<T>,
}

impl<T: PrimInt> ScalarDataType<T> {
    pub(crate) fn new(dt: DataType) -> Self {
        Self {
            data_type: dt,
            _marker: PhantomData,
        }
    }

    fn scalar(&self, r: Option<i64>) -> Result<ScalarValue> {
        match &self.data_type {
            DataType::Date32 => Ok(ScalarValue::Date32(r.and_then(|v| v.to_i32()))),
            DataType::Timestamp(u, tz) => match u {
                TimeUnit::Second => Ok(ScalarValue::TimestampSecond(r, tz.clone())),
                TimeUnit::Millisecond => {
                    Ok(ScalarValue::TimestampMillisecond(r, tz.clone()))
                }
                TimeUnit::Microsecond => {
                    Ok(ScalarValue::TimestampMicrosecond(r, tz.clone()))
                }
                TimeUnit::Nanosecond => {
                    Ok(ScalarValue::TimestampNanosecond(r, tz.clone()))
                }
            },
            t => Err(internal_datafusion_err!(
                "Unsupported data type for ScalarDataType<T>: {t:?}"
            )),
        }
    }
}

pub(crate) fn handle<O, F, T>(
    args: &[ColumnarValue],
    op: F,
    name: &str,
    sdt: &ScalarDataType<T>,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    T: PrimInt,
    F: Fn(&str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&StringViewArray, O, _>(
                    &a.as_string_view(),
                    op,
                )?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i64>, O, _>(
                    &a.as_string::<i64>(),
                    op,
                )?,
            ))),
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i32>, O, _>(
                    &a.as_string::<i32>(),
                    op,
                )?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let result = a
                    .as_ref()
                    .map(|x| op(x))
                    .transpose()?
                    .and_then(|v| v.to_i64());
                let s = sdt.scalar(result)?;
                Ok(ColumnarValue::Scalar(s))
            }
            _ => exec_err!("Unsupported data type {scalar:?} for function {name}"),
        },
    }
}

// Given a function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
pub(crate) fn handle_multiple<O, F, M, T>(
    args: &[ColumnarValue],
    op: F,
    op2: M,
    name: &str,
    sdt: &ScalarDataType<T>,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    F: Fn(&str, &str) -> Result<O::Native>,
    M: Fn(O::Native) -> O::Native,
    T: PrimInt,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                                // all good
                            }
                            other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                        },
                        ColumnarValue::Scalar(arg) => {
                            match arg.data_type() {
                                DataType::Utf8View| DataType::LargeUtf8 | DataType::Utf8 => {
                                    // all good
                                }
                                other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                            }
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<O, _, _>(args, op, op2, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let a = a.as_ref();
                // ASK: Why do we trust `a` to be non-null at this point?
                let a = unwrap_or_internal_err!(a);

                let mut ret = None;

                for (pos, v) in args.iter().enumerate().skip(1) {
                    let ColumnarValue::Scalar(
                        ScalarValue::Utf8View(x)
                        | ScalarValue::LargeUtf8(x)
                        | ScalarValue::Utf8(x),
                    ) = v
                    else {
                        return exec_err!("Unsupported data type {v:?} for function {name}, arg # {pos}");
                    };

                    if let Some(s) = x {
                        match op(a, s.as_str()) {
                            Ok(r) => {
                                let result = op2(r).to_i64();
                                let s = sdt.scalar(result)?;
                                ret = Some(Ok(ColumnarValue::Scalar(s)));
                                break;
                            }
                            Err(e) => ret = Some(Err(e)),
                        }
                    }
                }

                unwrap_or_internal_err!(ret)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
    }
}

/// given a function `op` that maps `&str`, `&str` to the first successful Result
/// of an arrow native type, returns a `PrimitiveArray` after the application of the
/// function to `args` and the subsequence application of the `op2` function to any
/// successful result. This function calls the `op` function with the first and second
/// argument and if not successful continues with first and third, first and fourth,
/// etc until the result was successful or no more arguments are present.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not > 1 or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<O, F, F2>(
    args: &[ColumnarValue],
    op: F,
    op2: F2,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(&str, &str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    if args.len() < 2 {
        return exec_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => {
                let string_array = a.as_string_view();
                handle_array_op::<O, &StringViewArray, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::LargeUtf8 => {
                let string_array = as_generic_string_array::<i64>(&a)?;
                handle_array_op::<O, &GenericStringArray<i64>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::Utf8 => {
                let string_array = as_generic_string_array::<i32>(&a)?;
                handle_array_op::<O, &GenericStringArray<i32>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            other => exec_err!(
                "Unsupported data type {other:?} for function substr,\
                    expected Utf8View, Utf8 or LargeUtf8."
            ),
        },
        other => exec_err!(
            "Received {} data type, expected only array",
            other.data_type()
        ),
    }
}

fn handle_array_op<'a, O, V, F, F2>(
    first: &V,
    args: &[ColumnarValue],
    op: F,
    op2: F2,
) -> Result<PrimitiveArray<O>>
where
    V: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&str, &str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    first
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;
            if let Some(x) = x {
                for arg in args {
                    let v = match arg {
                        ColumnarValue::Array(a) => match a.data_type() {
                            DataType::Utf8View => Ok(a.as_string_view().value(pos)),
                            DataType::LargeUtf8 => Ok(a.as_string::<i64>().value(pos)),
                            DataType::Utf8 => Ok(a.as_string::<i32>().value(pos)),
                            other => exec_err!("Unexpected type encountered '{other}'"),
                        },
                        ColumnarValue::Scalar(s) => match s.try_as_str() {
                            Some(Some(v)) => Ok(v),
                            Some(None) => continue, // null string
                            None => exec_err!("Unexpected scalar type encountered '{s}'"),
                        },
                    }?;

                    let r = op(x, v);
                    if let Ok(inner) = r {
                        val = Some(Ok(op2(inner)));
                        break;
                    } else {
                        val = Some(r);
                    }
                }
            };

            val.transpose()
        })
        .collect()
}

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the function `op` errors
fn unary_string_to_primitive_function<'a, StringArrType, O, F>(
    array: &StringArrType,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    StringArrType: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&'a str) -> Result<O::Native>,
{
    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}
