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

extern crate criterion;

use arrow::array::{Array, ArrayAccessor, GenericStringArray, OffsetSizeTrait, StringViewArray};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_functions::utils::{Iter, StringArrayType, StringArrays};

fn criterion_benchmark(c: &mut Criterion) {
    for size in [4096, 16384] {
        let mut group = c.benchmark_group("string_arrays benchmark");

        for str_len in [4, 32] {
            let string_view_array = create_string_view_array_with_len(size, 0.2, str_len, true);
            let string_array = create_string_array_with_len::<i32>(size, 0.2, str_len);

            group.bench_function(BenchmarkId::new(format!("StringArrays-iter-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        let array = StringArrays::try_from(&string_view_array).unwrap();
                        array.iter().for_each(|v| {
                            if let Some(v) = v {
                                let _val = v;
                            }
                        });

                        let array = StringArrays::try_from(&string_array).unwrap();
                        array.iter().for_each(|v| {
                            if let Some(v) = v {
                                let _val = v;
                            }
                        });
                    })
                })
            });
            group.bench_function(BenchmarkId::new(format!("StringArrays-using_loop-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        let array = StringArrays::try_from(&string_view_array).unwrap();
                        for i in 0..size {
                            if !array.is_null(i) {
                                let _val = array.value(i);
                            };
                        }

                        let array = StringArrays::try_from(&string_array).unwrap();
                        for i in 0..size {
                            if !array.is_null(i) {
                                let _val = array.value(i);
                            };
                        }
                    })
                })
            });

            group.bench_function(BenchmarkId::new(format!("direct-iter-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        string_view_array.iter().for_each(|v| {
                            if let Some(v) = v {
                                let _val = v;
                            }
                        });
                        string_array.iter().for_each(|v| {
                            if let Some(v) = v {
                                let _val = v;
                            }
                        });
                    })
                })
            });

            group.bench_function(BenchmarkId::new(format!("direct-using_loop-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        for i in 0..size {
                            if !string_view_array.is_null(i) {
                                let _val = string_view_array.value(i);
                            };
                        }

                        for i in 0..size {
                            if !string_array.is_null(i) {
                                let _val = string_array.value(i);
                            };
                        }
                    })
                })
            });

            group.bench_function(BenchmarkId::new(format!("StringArrayType-iter-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        fn test<'a, T, S>(string_array: S)
                        where
                            T: OffsetSizeTrait,
                            S: StringArrayType<'a>,
                        {
                            string_array.iter().for_each(|v| {
                                if let Some(v) = v {
                                    let _val = v;
                                }
                            });
                        }

                        test::<i32, &StringViewArray>(&string_view_array.clone());
                        test::<i32, &GenericStringArray<i32>>(&string_array.clone());
                    })
                })
            });

            group.bench_function(BenchmarkId::new(format!("StringArrayType-using_loop-{size}"), str_len), |b| {
                b.iter(|| {
                    black_box({
                        fn test<'a, T, S>(string_array: S, size: usize)
                        where
                            T: OffsetSizeTrait,
                            S: StringArrayType<'a>,
                        {
                            for i in 0..size {
                                if !string_array.is_null(i) {
                                    let _val = StringArrayType::value(&string_array, i);
                                };
                            }
                        }

                        test::<i32, &StringViewArray>(&string_view_array.clone(), size);
                        test::<i32, &GenericStringArray<i32>>(&string_array.clone(), size);
                    })
                })
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
