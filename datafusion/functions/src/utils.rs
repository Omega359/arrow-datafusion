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

use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayAccessor, ArrayData, ArrayIter, ArrayRef, AsArray, GenericStringArray,
    OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow_buffer::NullBuffer;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
///
/// If the input type is `Utf8View` the return type is $utf8Type,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        pub(crate) fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                // Utf8View max offset size is u32::MAX, the same as UTF8
                DataType::Utf8View | DataType::BinaryView => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return datafusion_common::exec_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return datafusion_common::exec_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_str_type`: returns either a Utf8 or LargeUtf8 based on the input type size.
get_optimal_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);

// `utf8_to_int_type`: returns either a Int32 or Int64 based on the input type size.
get_optimal_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub(super) fn make_scalar_function<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

/// An enum that holds string arrays that helps with writing generic code
/// that can work with different types of string arrays.
///
/// Currently three types of arrays are supported:
/// - `GenericStringArray<i32>`
/// - `GenericStringArray<i64>`
/// - `StringViewArray`
///
/// The use of this enum is useful since Rust currently disallows `StringArrayType` to be used
/// as an object (since it's Sized and uses self).
///
/// # Examples
/// ```
/// # use std::sync::Arc;
/// # use arrow::array::{StringArray, LargeStringArray, StringViewArray, ArrayAccessor, ArrayRef, ArrayIter};
/// # use datafusion_functions::utils::{StringArrayType, StringArrays};
///
/// /// Combines string values for any ArrayAccessor type. It can be invoked on
/// /// and combination of `StringArray`, `LargeStringArray` or `StringViewArray`
/// fn combine_values<'a>(array1: &StringArrays, array2: &StringArrays) -> Vec<String> {
///   // iterate over the elements of the 2 arrays in parallel
///   array1
///   .iter()
///   .zip(array2.iter())
///   .map(|(s1, s2)| {
///      // if both values are non null, combine them
///      if let (Some(s1), Some(s2)) = (s1, s2) {
///        format!("{s1}{s2}")
///      } else {
///        "None".to_string()
///     }
///    })
///   .collect()
/// }
///
/// // build a few arrays for this example
/// let string_array = StringArray::from(vec!["foo", "bar"]);
/// let large_string_array = LargeStringArray::from(vec!["foo2", "bar2"]);
/// let string_view_array = StringViewArray::from(vec!["foo3", "bar3"]);
///
/// // these arrayRef's are what a Udf is likely to receive
/// let array_ref_1 = Arc::new(string_array.clone()) as ArrayRef;
/// let array_ref_2 = Arc::new(large_string_array.clone()) as ArrayRef;
/// let array_ref_3 = Arc::new(string_view_array.clone()) as ArrayRef;
///
/// // Easily convert the different type of arrays using a single api.
/// let array_1 = StringArrays::try_from(&array_ref_1).unwrap();
/// let array_2 = StringArrays::try_from(&array_ref_2).unwrap();
/// let array_3 = StringArrays::try_from(&array_ref_3).unwrap();
///
/// // can invoke this function a any of the array types
/// assert_eq!(
///   combine_values(&array_1, &array_2),
///   vec![String::from("foofoo2"), String::from("barbar2")]
/// );
///
/// // Can call the same function with string array and string view array
/// assert_eq!(
///   combine_values(&array_1, &array_3),
///   vec![String::from("foofoo3"), String::from("barbar3")]
/// );
/// ```
///
/// This enum also implements ArrayIter` allowing for calls such as
/// ArrayIter::new(string_arrays)
///
/// or pass it into a method that expects an `ArrayAccessor`
/// do_something(&string_arrays)
///
/// To obtain a `StringArrays` object from a typical `&ArrayRef` use
/// let string_array = StringArrays::try_from(array)?;
pub enum StringArrays<'a> {
    StringView(&'a StringViewArray),
    String(&'a GenericStringArray<i32>),
    LargeString(&'a GenericStringArray<i64>),
}

pub trait Iter {
    fn iter(self) -> ArrayIter<Self>
    where
        Self: ArrayAccessor,
        Self: Sized;
}

impl<'a> Iter for StringArrays<'a> {
    fn iter(self) -> ArrayIter<Self> {
        ArrayIter::new(self)
    }
}

impl<'a> TryFrom<&'a ArrayRef> for StringArrays<'a> {
    type Error = DataFusionError;

    fn try_from(array: &'a ArrayRef) -> Result<Self, Self::Error> {
        match array.data_type() {
            DataType::Utf8 => Ok(StringArrays::from(array.as_string::<i32>())),
            DataType::LargeUtf8 => Ok(StringArrays::from(array.as_string::<i64>())),
            DataType::Utf8View => Ok(StringArrays::from(array.as_string_view())),
            other => {
                exec_err!("Unsupported data type {other:?} for function substr_index")
            }
        }
    }
}

impl<'a> From<&'a StringViewArray> for StringArrays<'a> {
    fn from(value: &'a StringViewArray) -> Self {
        StringArrays::StringView(value)
    }
}
impl<'a> From<&'a GenericStringArray<i32>> for StringArrays<'a> {
    fn from(value: &'a GenericStringArray<i32>) -> Self {
        StringArrays::String(value)
    }
}
impl<'a> From<&'a GenericStringArray<i64>> for StringArrays<'a> {
    fn from(value: &'a GenericStringArray<i64>) -> Self {
        StringArrays::LargeString(value)
    }
}

impl<'a> Array for StringArrays<'a> {
    fn as_any(&self) -> &dyn Any {
        match self {
            StringArrays::StringView(sv) => sv.as_any(),
            StringArrays::String(s) => s.as_any(),
            StringArrays::LargeString(s) => s.as_any(),
        }
    }

    fn to_data(&self) -> ArrayData {
        match self {
            StringArrays::StringView(sv) => sv.to_data(),
            StringArrays::String(s) => s.to_data(),
            StringArrays::LargeString(s) => s.to_data(),
        }
    }

    fn into_data(self) -> ArrayData {
        match self {
            StringArrays::StringView(sv) => sv.into_data(),
            StringArrays::String(s) => s.into_data(),
            StringArrays::LargeString(s) => s.into_data(),
        }
    }

    fn data_type(&self) -> &DataType {
        match self {
            StringArrays::StringView(sv) => sv.data_type(),
            StringArrays::String(s) => s.data_type(),
            StringArrays::LargeString(s) => s.data_type(),
        }
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        match self {
            StringArrays::StringView(sv) => sv.slice(offset, length),
            StringArrays::String(s) => s.slice(offset, length),
            StringArrays::LargeString(s) => s.slice(offset, length),
        }
    }

    fn len(&self) -> usize {
        match self {
            StringArrays::StringView(sv) => sv.len(),
            StringArrays::String(s) => s.len(),
            StringArrays::LargeString(s) => s.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            StringArrays::StringView(sv) => sv.is_empty(),
            StringArrays::String(s) => s.is_empty(),
            StringArrays::LargeString(s) => s.is_empty(),
        }
    }

    fn offset(&self) -> usize {
        match self {
            StringArrays::StringView(sv) => sv.offset(),
            StringArrays::String(s) => s.offset(),
            StringArrays::LargeString(s) => s.offset(),
        }
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        match self {
            StringArrays::StringView(sv) => sv.nulls(),
            StringArrays::String(s) => s.nulls(),
            StringArrays::LargeString(s) => s.nulls(),
        }
    }

    fn get_buffer_memory_size(&self) -> usize {
        match self {
            StringArrays::StringView(sv) => sv.get_buffer_memory_size(),
            StringArrays::String(s) => s.get_buffer_memory_size(),
            StringArrays::LargeString(s) => s.get_buffer_memory_size(),
        }
    }

    fn get_array_memory_size(&self) -> usize {
        match self {
            StringArrays::StringView(sv) => sv.get_array_memory_size(),
            StringArrays::String(s) => s.get_array_memory_size(),
            StringArrays::LargeString(s) => s.get_array_memory_size(),
        }
    }
}

impl<'a> Debug for StringArrays<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StringArrays::StringView(sv) => sv.fmt(f),
            StringArrays::String(s) => s.fmt(f),
            StringArrays::LargeString(s) => s.fmt(f),
        }
    }
}

impl<'a> Display for StringArrays<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StringArrays::StringView(sv) => sv.fmt(f),
            StringArrays::String(s) => s.fmt(f),
            StringArrays::LargeString(s) => s.fmt(f),
        }
    }
}

impl<'a> ArrayAccessor for &'a StringArrays<'a> {
    type Item = &'a str;

    fn value(&self, index: usize) -> Self::Item {
        match self {
            StringArrays::StringView(sv) => StringArrayType::value(sv, index),
            StringArrays::String(s) => GenericStringArray::<i32>::value(s, index),
            StringArrays::LargeString(s) => GenericStringArray::<i64>::value(s, index),
        }
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        match self {
            StringArrays::StringView(sv) => sv.value_unchecked(index),
            StringArrays::String(s) => s.value_unchecked(index),
            StringArrays::LargeString(s) => s.value_unchecked(index),
        }
    }
}

impl<'a> ArrayAccessor for StringArrays<'a> {
    type Item = &'a str;

    fn value(&self, index: usize) -> Self::Item {
        match self {
            StringArrays::StringView(sv) => StringArrayType::value(sv, index),
            StringArrays::String(s) => GenericStringArray::<i32>::value(s, index),
            StringArrays::LargeString(s) => GenericStringArray::<i64>::value(s, index),
        }
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        match self {
            StringArrays::StringView(sv) => sv.value_unchecked(index),
            StringArrays::String(s) => s.value_unchecked(index),
            StringArrays::LargeString(s) => s.value_unchecked(index),
        }
    }
}

impl<'a> StringArrayType<'a> for &'a StringArrays<'a> {
    fn iter(&self) -> ArrayIter<Self> {
        ArrayIter::new(self)
    }

    fn value(&self, index: usize) -> Self::Item {
        ArrayAccessor::value(self, index)
    }
}

pub trait StringArrayType<'a>: ArrayAccessor<Item = &'a str> + Sized {
    fn iter(&self) -> ArrayIter<Self>;

    fn value(&self, index: usize) -> Self::Item;
}

impl<'a, T: OffsetSizeTrait> StringArrayType<'a> for &'a GenericStringArray<T> {
    fn iter(&self) -> ArrayIter<Self> {
        GenericStringArray::<T>::iter(self)
    }

    fn value(&self, index: usize) -> Self::Item {
        GenericStringArray::<T>::value(self, index)
    }
}

impl<'a> StringArrayType<'a> for &'a StringViewArray {
    fn iter(&self) -> ArrayIter<Self> {
        StringViewArray::iter(self)
    }

    fn value(&self, index: usize) -> Self::Item {
        StringViewArray::value(self, index)
    }
}

#[cfg(test)]
pub mod test {
    /// $FUNC ScalarUDFImpl to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<ColumnarValue>
    /// $EXPECTED_TYPE is the expected value type
    /// $EXPECTED_DATA_TYPE is the expected result type
    /// $ARRAY_TYPE is the column type after function applied
    macro_rules! test_function {
        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
            let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
            let func = $FUNC;

            let type_array = $ARGS.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
            let return_type = func.return_type(&type_array);

            match expected {
                Ok(expected) => {
                    assert_eq!(return_type.is_ok(), true);
                    assert_eq!(return_type.unwrap(), $EXPECTED_DATA_TYPE);

                    let result = func.invoke($ARGS);
                    assert_eq!(result.is_ok(), true);

                    let len = $ARGS
                        .iter()
                        .fold(Option::<usize>::None, |acc, arg| match arg {
                            ColumnarValue::Scalar(_) => acc,
                            ColumnarValue::Array(a) => Some(a.len()),
                        });
                    let inferred_length = len.unwrap_or(1);
                    let result = result.unwrap().clone().into_array(inferred_length).expect("Failed to convert to array");
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().expect("Failed to convert to type");

                    // value is correct
                    match expected {
                        Some(v) => assert_eq!(result.value(0), v),
                        None => assert!(result.is_null(0)),
                    };
                }
                Err(expected_error) => {
                    if return_type.is_err() {
                        match return_type {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => { datafusion_common::assert_contains!(expected_error.strip_backtrace(), error.strip_backtrace()); }
                        }
                    }
                    else {
                        // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                        match func.invoke($ARGS) {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => {
                                assert!(expected_error.strip_backtrace().starts_with(&error.strip_backtrace()));
                            }
                        }
                    }
                }
            };
        };
    }

    use arrow::datatypes::DataType;
    #[allow(unused_imports)]
    pub(crate) use test_function;

    use super::*;

    #[test]
    fn string_to_int_type() {
        let v = utf8_to_int_type(&DataType::Utf8, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::Utf8View, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::LargeUtf8, "test").unwrap();
        assert_eq!(v, DataType::Int64);
    }
}
