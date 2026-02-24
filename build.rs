use std::env;
use std::fs::{self, read_dir, File};
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

const DATALOG_DIR: &str = "datalog";
const DATALOG_COMPILED: &str = "datalog_compiled";

const PROGRAM_LIST_FILE: &str = "src/transformation/souffle/souffle_ffi.rs";
const PROGRAM_LIST_TEMPLATE: &str = "src/transformation/souffle/souffle_ffi_template.rs";

fn create_program_list_file() -> BufWriter<File> {
    let list_file = File::create(PROGRAM_LIST_FILE).expect("Could not open program list file.");
    let mut template_list_file =
        File::open(PROGRAM_LIST_TEMPLATE).expect("Could not open program list template file.");
    let mut template = String::new();
    template_list_file
        .read_to_string(&mut template)
        .expect("Could not read template.");
    let mut writer = BufWriter::new(list_file);
    write!(writer, "{}\n", template).expect("Could not write to program list file.");
    writer
}

fn close_program_list_file(mut _writer: BufWriter<File>) {
    // write!(writer, "}}").expect("Could not write to program list file.");
}

fn register_program(writer: &mut BufWriter<File>, filepath: PathBuf, name: &str) {
    writeln!(
        writer,
        "
#[cxx::bridge(namespace = \"{name}\")]
pub mod {name}_ffi {{
    unsafe extern \"C++\" {{
        include!(\"{file}\");
        type factory_Sf_{name};
    }}
}}\
",
        file=filepath.to_str().expect("Invalid path."),
        name=name
    )
    .expect("Could not write to program list file.");
}

fn build() {
    let datalog_path = PathBuf::from(DATALOG_DIR)
        .canonicalize()
        .expect("No datalog directory.");

    let mut datalog_compiled_path = PathBuf::from(DATALOG_COMPILED);
    if !datalog_compiled_path.exists() {
        fs::create_dir_all(datalog_compiled_path.clone())
            .expect("Could not create directory {datalog_compiled_path}");
    }
    datalog_compiled_path = datalog_compiled_path
        .canonicalize()
        .expect("Error computing path for compiled directory.");

    let mut programs = vec![];
    let mut program_list_writer = create_program_list_file();
    for dir in read_dir(datalog_path.clone()).expect("Could not open datalog dir.") {
        let path = dir.expect("Could not read file.").path();
        if path
            .extension()
            .map(|ext| ext.to_str().map(|ext| ext.ends_with("dl")).unwrap_or(false))
            .unwrap_or(false)
        {
            let progname = path
                .file_stem()
                .expect("Invalid file.")
                .to_str()
                .expect("Encoding error.")
                .to_owned();
            if progname != "definitions" {
                let outpath = datalog_compiled_path.join(progname.clone() + ".cpp");
                programs.push(
                    outpath
                        .to_str()
                        .expect("Error building program name.")
                        .to_string(),
                );
                if !std::process::Command::new("souffle")
                    .arg("-g")
                    .arg(&outpath)
                    .arg("-N")
                    .arg(progname.clone())
                    .arg(path)
                    .output()
                    .expect("Could not find souffle.")
                    .status
                    .success()
                {
                    panic!("Could not generate souffle program.");
                }
                register_program(&mut program_list_writer, outpath, &progname);
            }
        }
    }
    close_program_list_file(program_list_writer);

    cxx_build::bridges(["src/transformation/souffle/souffle_ffi.rs"])
        .file("cpp_util/souffleUtil.hpp")
        .file("/usr/include/souffle/SouffleInterface.h")
        .files(programs)
        .cpp(true)
        .std("c++17")
        .flag("-fkeep-inline-functions")
        .define("__EMBEDDED_SOUFFLE__", None)
        .include(".")
        .compile("transProofSouffle");

    println!(
        "cargo:rerun-if-changed=src/transformation/souffle_ffi_template.rs"
    );
}

//fn main() {}

fn main() {
    let build_enabled = env::var("BUILD_ENABLED")
        .map(|v| v == "1")
        .unwrap_or(true); // run by default

    if build_enabled {
        build();
    }
}
