use arrow::array::AsArray;
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext};
use datafusion_common::Result;
use regex::Regex;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
pub struct SqlBenchmark {
    name: String,
    group: String,
    subgroup: String,
    benchmark_path: PathBuf,
    benchmark_config: String,
    replacement_mapping: HashMap<String, String>,
    queries: HashMap<String, String>,
    display_name: String,
    display_group: String,
    result_queries: Vec<BenchmarkQuery>,
    assert_queries: Vec<BenchmarkQuery>,
    is_loaded: bool,
}

impl SqlBenchmark {
    pub fn new(full_path: &str, benchmark_directory: &str) -> Self {
        let group_name = parse_group_from_path(full_path, benchmark_directory);
        let mut bm = Self {
            name: String::new(),
            group: group_name,
            subgroup: String::new(),
            benchmark_path: PathBuf::from(full_path),
            benchmark_config: full_path.to_string(),
            replacement_mapping: HashMap::new(),
            queries: HashMap::new(),
            display_name: String::new(),
            display_group: String::new(),
            result_queries: Vec::new(),
            assert_queries: Vec::new(),
            is_loaded: false,
        };
        bm.replacement_mapping
            .insert("BENCHMARK_DIR".to_string(), benchmark_directory.to_string());
        bm
    }

    pub fn initialize(&mut self, rt: &Runtime, ctx: &SessionContext) -> Result<()> {
        if self.is_loaded {
            return Ok(());
        }

        let path = self.benchmark_path.to_string_lossy().into_owned();
        self.process_file(&path)?;

        // validate there was a run query
        if !self.queries.contains_key("run") {
            panic!("Invalid benchmark file: no \"run\" query specified: {path}");
        }

        self.is_loaded = true;

        Ok(())
    }

    pub fn assert(&mut self, rt: &Runtime, ctx: &SessionContext) -> Result<()> {
        Ok(())
    }

    pub fn run(&mut self, rt: &Runtime, ctx: &SessionContext) -> Result<()> {
        Ok(())
    }

    pub fn verify(&mut self, rt: &Runtime, ctx: &SessionContext) -> Result<()> {
        Ok(())
    }

    pub fn cleanup(&mut self, rt: &Runtime, ctx: &SessionContext) -> Result<()> {
        Ok(())
    }

    /// Processes the benchmark file
    fn process_file(&mut self, path: &str) -> Result<()> {
        let replacement_mapping = self.replacement_mapping.clone();
        let mut reader = BenchmarkFileReader::new(path, replacement_mapping)?;
        let mut line = String::new();

        while reader.read_line(&mut line).is_some() {
            // Skip blank lines and comments.
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Split the line into lower‑cased tokens.
            let lc_line = line.to_lowercase();
            let splits: Vec<&str> = lc_line.split_whitespace().collect();

            match splits[0] {
                "load" | "run" | "init" | "cleanup" => {
                    if self.queries.contains_key(splits[0]) {
                        panic!(
                            "Multiple calls to {} in the same benchmark file",
                            splits[0]
                        );
                    }

                    // Read the query body until a blank line or EOF.
                    let mut query = String::new();
                    while reader.read_line(&mut line).is_some() {
                        if line.is_empty() {
                            break;
                        } else {
                            query.push_str(&line);
                            query.push(' ');
                        }
                    }

                    // Optional file parameter.
                    if splits.len() > 1 && !splits[1].is_empty() {
                        let mut file = File::open(splits[1])
                            .expect(&format!("Failed to open query file {}", splits[1]));
                        let mut buf = Vec::new();
                        file.read_to_end(&mut buf)
                            .expect(&format!("Failed to read query file {}", splits[1]));
                        query = String::from_utf8_lossy(&buf).to_string();
                    }

                    // Trim and validate.
                    query = query.trim().to_string();
                    if query.is_empty() {
                        panic!("Encountered an empty {} node!", splits[0]);
                    }

                    self.queries.insert(splits[0].to_string(), query);
                }
                "name" | "group" | "subgroup" => {
                    // Extract the display value from the line
                    let result = line[splits[0].len() + 1..].trim().to_string();
                    match splits[0] {
                        "name" => self.display_name = result,
                        "group" => self.display_group = result,
                        "subgroup" => self.subgroup = result,
                        _ => {}
                    }
                }
                "assert" => {
                    // count the amount of columns
                    if splits.len() <= 1 || splits[1].len() == 0 {
                        panic!("{}", reader.format_exception("assert must be followed by a column count (e.g. result III)"))
                    }

                    // read the actual query
                    let mut found_end = false;
                    let mut sql = String::new();

                    while reader.read_line(&mut line).is_some() {
                        if (line == "----") {
                            found_end = true;
                            break;
                        }
                        sql.push('\n');
                        sql.push_str(&line);
                    }
                    if !found_end {
                        panic!("{}", reader.format_exception("assert must be followed by a query and a result (separated by ----)"));
                    }

                    self.assert_queries.push(read_query_from_reader(
                        &mut reader,
                        &sql,
                        splits[1],
                    )?);
                }
                "result_query" | "result" => {
                    if splits.len() <= 1 || splits[1].is_empty() {
                        panic!("{}", reader.format_exception(
                            "result must be followed by a column count (e.g. result III)"
                        ))
                    }

                    let is_file = splits[1].chars().any(|c| c != 'i');
                    let mut matches_condition = true;

                    if splits.len() > 2 {
                        for condition in &splits[2..] {
                            if !condition.contains('=') {
                                panic!("{}", reader.format_exception(
                                    "result with condition - only = is supported currently"
                                ));
                            }

                            let condition_parts: Vec<&str> =
                                condition.splitn(2, '=').collect();
                            if condition_parts.len() != 2 {
                                panic!(
                                    "{}",
                                    reader.format_exception(
                                        "result with condition must have one equality"
                                    )
                                );
                            }

                            let condition_arg = condition_parts[0];
                            let condition_val = condition_parts[1];

                            match self.replacement_mapping.get(condition_arg) {
                                Some(val) if val != condition_val => {
                                    matches_condition = false;
                                    break;
                                }
                                None => panic!(
                                    "{}",
                                    reader.format_exception(&format!(
                                        "Condition argument '{}' not found in benchmark",
                                        condition_arg
                                    ))
                                ),
                                _ => {}
                            }
                        }
                    }

                    let mut result_query = String::new();

                    if splits[0] == "result_query" {
                        let mut sql = String::new();
                        let mut found_end = false;

                        while reader.read_line(&mut line).is_some() {
                            if line.trim() == "----" {
                                found_end = true;
                                break;
                            }
                            sql.push_str(&line);
                            sql.push('\n');
                        }

                        if !found_end {
                            panic!("{}", reader.format_exception(
                                "result_query must be followed by a query and a result (separated by ----)"
                            ));
                        }

                        result_query = sql;
                    } else {
                        result_query = "select * from __answer".to_string();
                    }

                    let result_check = if is_file {
                        if matches_condition {
                            let mut file = splits[1].to_string();
                            read_query_from_file(&mut file, &self.replacement_mapping)?
                        } else {
                            BenchmarkQuery {
                                query: String::new(),
                                column_count: 0,
                                expected_result: Vec::new(),
                            }
                        }
                    } else {
                        read_query_from_reader(&mut reader, &result_query, splits[1])?
                    };

                    if matches_condition {
                        if !self.result_queries.is_empty() {
                            panic!(
                                "{}",
                                reader.format_exception("multiple results found")
                            );
                        }
                        self.result_queries.push(result_check);
                    }
                }
                "template" => {
                    // template: update the path to read
                    self.benchmark_path = PathBuf::from(splits[1]);
                    // now read parameters
                    while reader.read_line(&mut line).is_some() {
                        if line.is_empty() {
                            break;
                        }
                        let parameters: Vec<&str> = line.split("=").collect();
                        if parameters.len() != 2 {
                            panic!(
                                "{}",
                                reader.format_exception(
                                    "Expected a template parameter in the form of X=Y"
                                )
                            );
                        }
                        self.replacement_mapping
                            .insert(parameters[0].to_string(), parameters[1].to_string());
                    }
                    // restart the load from the template file
                    return self.process_file(splits[1]);
                }
                "include" => {
                    if splits.len() != 2 {
                        panic!(
                            "{}",
                            reader.format_exception("include requires a single argument")
                        )
                    }
                    self.process_file(splits[1])?;
                }
                _ => {
                    // Unknown command; ignore or panic
                    panic!(
                        "{}",
                        reader.format_exception(&format!(
                            "Unrecognized command: {}",
                            splits[0]
                        ))
                    );
                }
            }

            // Clear the line buffer for the next iteration.
            line.clear();
        }

        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn subgroup(&self) -> &str {
        &self.subgroup
    }

    pub fn benchmark_path(&self) -> &PathBuf {
        &self.benchmark_path
    }

    pub fn benchmark_config(&self) -> &str {
        &self.benchmark_config
    }

    pub fn replacement_mapping(&self) -> &HashMap<String, String> {
        &self.replacement_mapping
    }

    pub fn queries(&self) -> &HashMap<String, String> {
        &self.queries
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn display_group(&self) -> &str {
        &self.display_group
    }

    pub fn result_queries(&self) -> &Vec<BenchmarkQuery> {
        &self.result_queries
    }

    pub fn assert_queries(&self) -> &Vec<BenchmarkQuery> {
        &self.assert_queries
    }

    pub fn is_loaded(&self) -> bool {
        self.is_loaded
    }
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Parse a group name from a file path
fn parse_group_from_path(path: &str, benchmark_directory: &str) -> String {
    let mut group_name: Vec<String> = vec![];

    loop {
        let path_buf = PathBuf::from(path);
        let mut parent = path_buf.parent();

        match parent {
            Some(p) => {
                if p.display()
                    .to_string()
                    .eq_ignore_ascii_case(benchmark_directory)
                {
                    break;
                } else {
                    let dir_name = p.display().to_string();
                    group_name.insert(0, dir_name);
                }
                parent = p.parent();
            }
            None => break,
        }
    }

    group_name.join("-")
}

/// Replace all `${KEY}` placeholders in a string according to the mapping.
fn process_replacements(str: &mut String, replacement_map: &HashMap<String, String>) {
    for (k, v) in replacement_map {
        let placeholder = "${".to_string() + k + "}";
        *str = str.replace(&placeholder, v);
    }

    // look for any remaining occurrences of ${..} and search in env variables for the
    // 'key's
    let regex = Regex::new("\\$\\{(.*)}").unwrap();
    let new_str = str.clone();
    regex.captures_iter(&new_str).for_each(|c| {
        let key = c.get(1).unwrap().as_str();
        let e = std::env::var(key.to_uppercase());
        match e {
            Ok(v) => {
                let placeholder = "${".to_string() + key + "}";
                *str = str.replace(&placeholder, &v);
            }
            Err(_) => (),
        }
    });
}

fn read_query_from_reader(
    reader: &mut BenchmarkFileReader,
    sql: &str,
    header: &str,
) -> Result<BenchmarkQuery> {
    let column_count = header.len();
    let mut expected_result = Vec::new();

    let mut line = String::new();
    while reader.read_line(&mut line).is_some() {
        if line.is_empty() {
            break;
        }

        let result_splits: Vec<&str> = line.split('\t').collect();
        if result_splits.len() != column_count {
            panic!(
                "{}",
                reader.format_exception(&format!(
                    "expected {} values but got {}",
                    result_splits.len(),
                    column_count
                )),
            );
        }

        expected_result.push(result_splits.into_iter().map(|s| s.to_string()).collect());
    }

    Ok(BenchmarkQuery {
        query: sql.to_string(),
        column_count,
        expected_result,
    })
}

fn read_query_from_file(
    path: &mut String,
    replacement_mapping: &HashMap<String, String>,
) -> Result<BenchmarkQuery> {
    // Process replacements in file path
    process_replacements(path, replacement_mapping);

    let rt = Runtime::new()?;
    let ctx = SessionContext::new();

    // Read CSV using DataFusion's read_csv function
    // Similar to DuckDB's read_csv with options
    let df: DataFrame = rt.block_on(async {
        ctx.read_csv(
            path.to_string(),
            CsvReadOptions::new()
                .has_header(true)
                .delimiter(b'|')
                .null_regex(Some("NULL".to_string()))
                .schema_infer_max_records(100),
        )
            .await
    })?;

    // Get schema to determine column count
    let schema = df.schema();
    let column_count = schema.fields().len();

    // Collect results into a Vec<Vec<String>>
    let mut expected_result = Vec::new();

    // Execute and collect results
    let batches = rt.block_on(async {
        df.collect().await
    })?;

    // Convert record batches to string vectors
    for record_batch in batches {
        for row_idx in 0..record_batch.num_rows() {
            let mut row_values = Vec::new();

            for col_idx in 0..record_batch.num_columns() {
                let array = record_batch.column(col_idx);
                // Convert array value to string
                let value = array.as_string::<i32>().value(row_idx);
                row_values.push(value.into());
            }

            expected_result.push(row_values);
        }
    }

    Ok(BenchmarkQuery {
        query: String::new(),
        column_count,
        expected_result,
    })
}

// ---------------------------------------------------------------------------
// BenchmarkFileReader – reads a benchmark file line by line
// ---------------------------------------------------------------------------

struct BenchmarkFileReader {
    path: PathBuf,
    reader: BufReader<File>,
    line_nr: usize,
    replacements: HashMap<String, String>,
}

impl BenchmarkFileReader {
    fn new<P: Into<PathBuf>>(
        path: P,
        replacement_map: HashMap<String, String>,
    ) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().read(true).open(&path)?;

        Ok(Self {
            path,
            reader: BufReader::new(file),
            line_nr: 0,
            replacements: replacement_map,
        })
    }

    /// Read the next line, applying replacements and trimming.
    fn read_line(&mut self, line: &mut String) -> Option<std::io::Result<String>> {
        match self.reader.read_line(line) {
            Ok(0) => None,
            Ok(_) => {
                self.line_nr += 1;
                // Trim newline and carriage return
                if line.ends_with('\n') {
                    line.pop();
                }
                if line.ends_with('\r') {
                    line.pop();
                }
                process_replacements(line, &self.replacements);
                Some(Ok(line.clone()))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn format_exception(&self, msg: &str) -> String {
        format!("{}:{} - {}", self.path.display(), self.line_nr, msg)
    }
}

#[derive(Debug, Clone)]
struct BenchmarkQuery {
    query: String,
    column_count: usize,
    expected_result: Vec<Vec<String>>,
}

impl BenchmarkQuery {
    fn new() -> Self {
        Self {
            query: String::new(),
            column_count: 0,
            expected_result: Vec::new(),
        }
    }
}
