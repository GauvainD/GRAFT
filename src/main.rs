use docopt::Docopt;
use log::{debug, error, info};
use neo4j::add_label;
use serde::Deserialize;
use std::fs::File;
use std::io::{stdin, BufRead, BufReader, Read};
use std::sync::mpsc::{channel, sync_channel};
use std::thread;
use std::time::Instant;
use transproof::neo4j;
use transproof::neo4j::{remove_label, save_timings, Neo4jConfig, SourceSelectorEnum, NEW_LABEL};

use transproof::compute::*;
use transproof::constants::{IDEMPOTENCE, MINHASH, NUM_BEST, PATH_WEIGHT, TOTAL_TIME};
use transproof::errors::*;
use transproof::transformation::*;

use transproof::{parsing::PropertyGraphParser, property_graph::PropertyGraph};

const USAGE: &str = "
Transrust is a tool to compute the results of different transformations on a given set of graphs.
These graphs have to be given in graph6 format from the input (one signature per line) and the
result is outputed in csv format.

Usage:
    transrust [options] <program>
    transrust (-h | --help)

Options:
    -h, --help             Show this message.
    -v, --verbose          Shows more information.
    -i, --input <input>    File containing the input schemas. Uses the standard input if '-'.
                           [default: -]
    -o, --output <output>  File where to write the result. Uses the standard output if '-'.
                           [default: -]
    -s, --buffer <buffer>  Size of the buffer [default: 2000000000]
    -t <threads>           Number of threads to be used for computation. A value of 0 means using
                           as many threads cores on the machine. [default: 0]
    -c <channel>           Size of the buffer to use for each threads (in number of messages). If
                           the size is 0, the buffer is unlimited. Use this if you have memory
                           issues even while setting a smaller output buffer and batch size.
                           [default: 0]
    -a, --append           Does not overwrite output file but appends results instead.
    --neo4j                Writes the output in a Neo4j database and proceed to multiple loops. Incompatible with -o.
    -l, --label <label>    Reads graphs from metanodes in Neo4j database having the given label. Incompatible with -i.
    --target <target>      File containing the target schema.
    -p, --prune <prune>    Number of best results to keep. [default: 6]
    --strat <strategy>     Strategy to use for the computation. Available strategies are: naive, random, weighted_distance and greedy. [default: greedy]
    -w, --weight <weight>  Weight to give to the distance in the weighted distance strategy. Must be between 0 and 1. [default: 0.5]
    --minshash <sample>    Use minhash similarity with the given sample size instead of default jaccard index. [default: 200]
    --idempotent           Operations are idempotent
    --theta <sim>          Minimum similarity to be considered as a solution. [default: 1.0]
    --turns <turns>        Maximum number of iterations without minimal improvement before giving up. [default: 5]
    --improv <improv>      Minimum improvement to reset the number of iterations without improvement. [default: 0.01]
    --neo4j-uri <uri>      URI of the Neo4j instance. [default: localhost:7687]
    --neo4j-user <user>    Neo4j username. [default: ]
    --neo4j-pass <pass>    Neo4j password. [default: ]
    ";

#[derive(Debug, Deserialize, Clone)]
struct Args {
    flag_v: bool,
    flag_i: String,
    flag_o: String,
    flag_s: usize,
    arg_program: String,
    flag_t: usize,
    flag_c: usize,
    flag_append: bool,
    flag_neo4j: bool,
    flag_target: Option<String>,
    flag_l: Option<String>,
    flag_p: Option<usize>,
    flag_strat: String,
    flag_w: f64,
    flag_minshash: Option<usize>,
    flag_idempotent: bool,
    flag_theta: f64,
    flag_turns: usize,
    flag_improv: f64,
    flag_neo4j_uri: String,
    flag_neo4j_user: String,
    flag_neo4j_pass: String,
}

fn main() -> Result<(), TransProofError> {
    let start = Instant::now();
    // Parsing args
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    let verbose = args.flag_v;

    // Initialize the program
    let prog = souffle::create_program_instance(&args.arg_program);

    // Init logger
    let debug_level = if verbose { "debug" } else { "info" };
    let env = env_logger::Env::default().filter_or("RUST_LOG", debug_level);
    let mut builder = env_logger::Builder::from_env(env);
    if !verbose {
        builder.default_format_module_path(false);
    }
    builder.init();
    debug!("{:?}", args);

    let filename = args.flag_i;
    let outfilename = args.flag_o;
    let buffer = args.flag_s;
    let num_threads = args.flag_t;
    let channel_size = args.flag_c;
    let append = args.flag_append;
    let program = args.arg_program;
    let neo4j = args.flag_neo4j;
    let label = args.flag_l;
    let max_turns = args.flag_turns;
    let min_improv = args.flag_improv;
    let neo4j_config = Neo4jConfig::new(args.flag_neo4j_uri, args.flag_neo4j_user, args.flag_neo4j_pass);
    let strat: SourceSelectorEnum = match &args.flag_strat.as_str() {
        &"random" => SourceSelectorEnum::Random,
        &"naive" => SourceSelectorEnum::Naive,
        &"weighted_distance" => SourceSelectorEnum::WeightedDistance,
        &"greedy" => SourceSelectorEnum::Greedy,
        _ => panic!("Unknown strategy"),
    };
    NUM_BEST
        .set(args.flag_p.unwrap())
        .expect("Failed to set NUM_BEST");

    MINHASH
        .set(args.flag_minshash)
        .expect("Failed to set MINHASH");

    if args.flag_w < 0.0 || args.flag_w > 1.0 {
        error!("Weight must be in the range [0,1].");
        panic!("Weight must be in the range [0,1].");
    }

    PATH_WEIGHT
        .set(args.flag_w)
        .expect("Failed to set PATH_WEIGHT");

    if filename != "-" && label.is_some() {
        error!("Option -L is not compatible with -i.");
        panic!("Option -L is not compatible with -i.");
    }

    IDEMPOTENCE
        .set(args.flag_idempotent)
        .expect("Failed to set IDEMPOTENCE");
    let target_graph: Option<PropertyGraph> = args
        .flag_target
        .map(|fname| -> Result<PropertyGraph, std::io::Error> {
            let mut buf = BufReader::new(File::open(fname)?);
            let mut text = String::new();
            buf.read_to_string(&mut text)?;
            let parser = PropertyGraphParser;
            let mut v = parser.convert_text(&text);
            if v.len() != 1 {
                error!("Only one target schema is supported. Found {}.", v.len());
                panic!("Only one target schema is supported. Found {}.", v.len());
            }
            let target = v.drain(0..1).next().unwrap();
            Ok(target)
        })
        .transpose()
        .unwrap();

    if (outfilename != "-" || append) && neo4j {
        error!("Option --neo4j is not compatible with -o or -a.");
        panic!("Option --neo4j is not compatible with -o or -a.");
    }

    // Init input
    let mut buf: Box<dyn BufRead> = match filename.as_str() {
        "-" => Box::new(BufReader::new(stdin())),
        _ => Box::new(BufReader::new(File::open(filename)?)),
    };

    // Init thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()?;

    let mut looping = neo4j;
    let mut turns = 0;
    let mut previous_sim = None;
    let mut previous_sig = None;
    let mut first_run = true;
    while (first_run || looping) && turns < max_turns {
        // Init comunications with sink thread
        let result_sender;
        let result_receiver;
        if channel_size == 0 {
            let result_chan = channel::<LogInfo>();
            result_sender = SenderVariant::from(result_chan.0);
            result_receiver = result_chan.1;
        } else {
            let result_chan = sync_channel::<LogInfo>(channel_size);
            result_sender = SenderVariant::from(result_chan.0);
            result_receiver = result_chan.1;
        }
        let builder = thread::Builder::new();
        let whandle;
        if neo4j {
            let config = neo4j_config.clone();
            whandle = builder.spawn(move || output_neo4j(result_receiver, first_run, config))?;
        } else {
            let outfilename = outfilename.clone();
            whandle =
                builder.spawn(move || output(result_receiver, outfilename, buffer, append))?;
        }

        // Obtain the schemas to be transformed
        let mut v;
        // If we had a previous run
        if looping {
            match previous_sim.zip(previous_sig) {
                Some((sim, sig)) => {
                    info!("Best similarity so far: {}", sim);
                    info!("Reached by: {}", sig as i64);
                }
                None => info!("First run"),
            }
        }
        // Select schemas using the strategy
        if neo4j && (label.is_some() || (looping && previous_sim.is_some())) {
            let mut schemas = neo4j::get_source_graphs(
                &label.clone().unwrap_or(neo4j::NEW_LABEL.to_string()),
                &strat,
                &neo4j_config,
            );
            let ids: Vec<i64> = schemas.iter().map(|x| x.0).collect();
            v = schemas.drain(..).map(|x| x.1.clone()).collect();
            remove_label(NEW_LABEL, &ids, &neo4j_config);
        } else { //First run reading from input
            let parser = PropertyGraphParser;
            let mut text = String::new();
            buf.read_to_string(&mut text)?;
            v = parser.convert_text(&text);
        }
        // Apply the transformations
        if !v.is_empty() {
            handle_graphs(
                &program,
                v,
                result_sender.clone(),
                target_graph.clone(),
            )?;
        } else {
            info!("No schemas left to transform.");
            looping = false;
        }
        drop(result_sender);

        // Update the best similarity and best schema found
        let (best_sim, best_sig) = whandle.join().map_err(|x| TransProofError::Thread(x))??;
        if let Some(best_sim_raw) = best_sim {
            if let Some(best_sig_raw) = best_sig {
                if let Some(previous_sim_raw) = previous_sim {
                    if (best_sim_raw - previous_sim_raw) < min_improv {
                        turns += 1;
                    } else {
                        turns = 0;
                    }
                    if best_sim_raw > previous_sim_raw {
                        previous_sim = Some(best_sim_raw);
                        previous_sig = Some(best_sig_raw);
                    }
                } else {
                    previous_sim = Some(best_sim_raw);
                    previous_sig = Some(best_sig_raw);
                }
                looping = looping
                    && previous_sim
                        .map(|sim| sim < args.flag_theta)
                        .unwrap_or(true);
            } else {
                println!("No best sig");
                looping = false;
            }
        } else {
            println!("No best sim");
            looping = false;
        }
        first_run = false;
    }

    // Output results or store them in the database
    if let Some((best_sim, best_sig)) = previous_sim.zip(previous_sig) {
        if neo4j {
            add_label(neo4j::TARGET_LABEL, best_sig, &neo4j_config);
        }
        info!("Final best similarity: {}", best_sim);
        info!("Reached by: {}", best_sig as i64);
    }
    {
        let mut time = TOTAL_TIME.lock().unwrap();
        *time += start.elapsed();
        // *TOTAL_TIME = *TOTAL_TIME + start.elapsed();
    }
    if neo4j {
        neo4j::compute_paths(
            neo4j::SOURCE_LABEL,
            neo4j::TARGET_LABEL,
            neo4j::OPERATIONS_PROP,
            &neo4j_config,
        );
        save_timings(&neo4j_config);
    }
    Ok(())
}
