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

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::error::Result;
use datafusion_benchmarks::sql_benchmark::SqlBenchmark;
use std::collections::HashMap;
use std::{env, fs};
use tokio::runtime::Runtime;
use datafusion::prelude::{SessionConfig, SessionContext};

static SQL_BENCHMARK_DIRECTORY: &str = "sql_benchmarks";

pub fn bench(c: &mut Criterion) {
    // retrieve the benchmark we wish to run (corresponds to SqlBenchmark::group_name())
    let bench_name: Option<String> = env::var("BENCH_NAME").ok();
    let result = load_benchmarks(SQL_BENCHMARK_DIRECTORY);
    let mut benchmarks = match result {
        Ok(benchmarks) => benchmarks,
        Err(err) => {
            panic!("failed load benchmarks: {err:?}")
        }
    };

    // filter to just the benchmarks that we want to run
    let benchmarks_to_run: HashMap<String, Vec<SqlBenchmark>> = match bench_name {
        Some(bench_name) => benchmarks
            .iter()
            .filter(|(key, val)| key.eq_ignore_ascii_case(&bench_name))
            .map(|(key, val)| (String::from(key), val.clone()))
            .collect(),
        None => benchmarks,
    };

    let rt = make_runtime();

    for (group, benchmarks) in benchmarks_to_run {
        let mut group = c.benchmark_group(group);
        for mut benchmark in benchmarks {
            // create a context
            let ctx = make_ctx();
            // initialize the benchmark. This parses the benchmark file and does any pre-execution
            // work such as loading data into tables
            benchmark.initialize(&rt, &ctx).expect("initialization failed");
            // run assertions
            benchmark.assert(&rt, &ctx).expect("assertion failed");

            // run the benchmark
            let mut cloned = benchmark.clone();
            group.bench_function(benchmark.name(), |b| b.iter(|| cloned.run(&rt, &ctx)));

            // verify the results
            benchmark.verify(&rt, &ctx);
            // run cleanup
            benchmark.cleanup(&rt, &ctx);
        }
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn make_ctx() -> SessionContext {
    let config = SessionConfig::from_env().unwrap();
    SessionContext::new_with_config(config)
}

/// Recursively walks the directory tree starting at `path` and
/// calls the call back function for every file encountered.
pub fn list_files<F>(path: &str, callback: &mut F)
where
    F: FnMut(&str),
{
    for dir_entry in fs::read_dir(path).unwrap() {
        let path = dir_entry.unwrap().path();
        if path.is_dir() {
            // Recurse into the sub‑directory
            list_files(&path.to_string_lossy(), callback);
        } else {
            // For files, invoke the callback with the full path as a string
            let full_str = path.to_string_lossy();
            callback(&full_str);
        }
    }
}

/// Loads all benchmark files in the `sql_benchmarks` directory.
/// For each file ending with `.benchmark` it creates a new
/// `SqlBenchmark` instance.
pub fn load_benchmarks(path: &str) -> Result<HashMap<String, Vec<SqlBenchmark>>> {
    let mut benches = HashMap::new();

    list_files(path, &mut |path: &str| {
        if path.ends_with(".benchmark") {
            // Replace this with the actual benchmark loading logic.
            // e.g., let _bench = InterpretedBenchmark::new(path);
            println!("Loading benchmark from {}", path);

            let benchmark = SqlBenchmark::new(path, SQL_BENCHMARK_DIRECTORY);
            let entries = benches
                .entry(benchmark.group().to_string())
                .or_insert(vec![]);
            entries.push(benchmark);
        }
    });

    Ok(benches)
}
