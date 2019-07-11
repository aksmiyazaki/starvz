#!/usr/bin/Rscript
library(starvz)
library(sparklyr)
library(dplyr)
library(arrow)

source("/home/aksmiyazaki/git/starvz/R_package/R/phase1_spark.R")

##############################
# Usage                      #
##############################
base_usage <- function(){
    stop('Usage: Call it with one of the following params:\n  -S For sequential execution.\n  -D for distributed spark execution.')
}

sequential_usage <- function ()
{
    stop("Usage: pre-workflow.R <directory> [application](optional)
            where <directory> contains CSV files of the workflow;
            where [application](optional) is either cholesky or qrmumps.
            If you want to run distributed, run with -D as first param.\n\n", call.=FALSE)
}

##############################
# Usage  with spark          #
##############################
spark_usage <- function()
{
    stop("Usage: pre-workflow.R -D <hdfs_directory> [spark_master][application](optional) [spark_home](optional)
           where -D means distributed over spark/hadoop execution;
           where <hdfs_directory> is the HDFS directory which contains large CSV files of the workflow [paje.state.csv];
           where <local_directory> is the local directory which contains small CSV files of the workflow [entities.csv];
           where [spark_master] is the MASTER parameter of a sparklyr connection;
           where [application](optional) is either cholesky or qrmumps;
           where [spark_home](optional) is the location of spark (only needed if SPARK_HOME environment variable isn't set);", call.=FALSE)
}

applicationMode <- function() {
    list(sequential = "Sequential", distributed = "Distributed")
}

debugging <- FALSE;

# Get the arguments to this script
if(debugging == FALSE)
{
    args = commandArgs(trailingOnly=TRUE)
    
    if (length(args) < 1 || (args[[1]] != '-D' && args[[1]] != '-S')){
        base_usage();
    }
    
    if(args[[1]] == '-D'){
        if(length(args) < 4){
            spark_usage();
        }
        
        input.mode <- applicationMode()$distributed;
        input.hdfs_dir <- args[[2]];
        input.local_dir <- args[[3]];
        if(length(args) > 3)
            input.spark_master <- args[[4]]
        if(length(args) > 4)
            input.application <- args[[5]]
        if(length(args) > 5)
            input.spark_home <- args[[6]]
        else
            input.spark_home <- NULL;
    }else{
        if(length(args) < 3){
            sequential_usage();
        }
           
        input.mode <- applicationMode()$sequential;
        if (length(args) < 2) {
            input.application = "";
        }else{
            input.application = args[[3]];
        }
        input.directory = args[[2]];
        if ( is.null(input.directory) ){
            usage();
        }
        setwd(input.directory);
    }
}else{
    input.spark_master <- 'yarn';
    input.mode <- applicationMode()$distributed;
    input.hdfs_dir <- '/user/aksmiyazaki/inputs';
    input.local_dir <- '/home/aksmiyazaki/tcc_data/5-v8-4_chifflet_8_6_2_dmdas_dpotrf_4_96000_960_false_ETHERNET10GB_true_r21909_10.dir/5-v8-4_chifflet_8_6_2_dmdas_dpotrf_4_96000_960_false_ETHERNET10GB_true_r21909_10_fxt'
    input.application <- 'cholesky';
    input.spark_home <- NULL;
}

if (input.application == "cholesky"){
    states.fun = cholesky_colors;
    states.filter = 2;
}else if (input.application == "qrmumps") {
    states.fun = qrmumps_colors;
    states.filter = 1;
}else if (input.application == "cholesky_pastix") {
    states.fun = cholesky_pastix_colors;
    states.filter = 1;
}else if (input.application == "cfd") {
        states.fun = cfd_colors;
        states.filter = 1;
}else if (input.application == "") {
        states.fun = cfd_colors;
        states.filter = 0;
}
    
overall_start <- as.numeric(Sys.time())
if(input.mode == applicationMode()$sequential){
    start_time <- as.numeric(Sys.time())
    data <- the_reader_function (directory = input.directory,
                                 app_states_fun = states.fun,
                                 state_filter = states.filter,
                                 whichApplication = input.application);
    end_time <- as.numeric(Sys.time())
    loginfo(paste("[Reading and parsing all files took", paste0("{",(end_time - start_time), "s}]")));
}else{
    start_time <- as.numeric(Sys.time())
    
    sc <- setup_spark_env(spark_master = input.spark_master,
                          spark_home = input.spark_home);
    end_time <- as.numeric(Sys.time())
    loginfo(paste("[The spark setup took", paste0("{",end_time - start_time, "s}]")));
    
    start_time <- as.numeric(Sys.time())
    data <- spark_reader_function(sc = sc,
                                  hdfs_directory = input.hdfs_dir,
                                  local_directory = input.local_dir,
                                  app_states_fun = states.fun,
                                  state_filter = states.filter,
                                  whichApplication = input.application);
    end_time <- as.numeric(Sys.time())
    loginfo(paste("[Reading and parsing all files took", paste0("{",end_time - start_time, "s}]")));
}


loginfo("Start Output Writting");

start_time <- as.numeric(Sys.time())
# State
if(input.mode == applicationMode()$sequential)
{
    filename <- "pre.state.parquet";
    loginfo(paste("Writing", filename));
    if (!is.null(data$State)){
        write_parquet(data$State, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.variable.parquet";
    loginfo(filename);
    if (!is.null(data$Variable)){
        write_parquet(data$Variable, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.link.parquet";
    loginfo(filename);
    if (!is.null(data$Link)){
        write_parquet(data$Link, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.dag.parquet";
    loginfo(filename);
    if (!is.null(data$DAG)){
        write_parquet(data$DAG, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.y.parquet";
    loginfo(filename);
    if (!is.null(data$Y)){
        write_parquet(data$Y, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.atree.parquet";
    loginfo(filename);
    if (!is.null(data$ATree)){
        write_parquet(data$ATree, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.gaps.parquet";
    loginfo(filename);
    if (!is.null(data$Gaps)){
        write_parquet(data$Gaps, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    filename <- "pre.pmtool.parquet";
    loginfo(filename);
    if (!is.null(data$pmtool)){
        write_parquet(data$pmtool, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    filename <- "pre.pmtool_states.parquet";
    loginfo(filename);
    if (!is.null(data$pmtool_states)){
        write_parquet(data$pmtool_states, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    # Data Rec
    filename <- "pre.data_handles.parquet";
    loginfo(filename);
    if (!is.null(data$data_handles)){
        write_parquet(data$data_handles, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    # Tasks Rec
    filename <- "pre.tasks.parquet";
    loginfo(filename);
    if (!is.null(data$tasks)){
        write_parquet(data$tasks, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.task_handles.parquet";
    loginfo(filename);
    if (!is.null(data$task_handles)){
        write_parquet(data$task_handles, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
    
    filename <- "pre.events.parquet";
    loginfo(filename);
    if (!is.null(data$Events)){
        write_parquet(data$Events, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }
}else{
    filename <- paste0(input.hdfs_dir, '/', "pre.state.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$State)){
        spark_write_parquet(data$State, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    } 
    
    filename <- paste0(input.hdfs_dir, '/', "pre.variable.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$Variable)){
        spark_write_parquet(data$Variable, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    filename <- paste0(input.hdfs_dir, '/', "pre.link.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$Link)){
        spark_write_parquet(data$Link, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    filename <- paste0(input.hdfs_dir, '/', "pre.dag.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$DAG)){
        spark_write_parquet(data$DAG, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    filename <- paste0(input.hdfs_dir, '/', "pre.y.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$Y)){
        spark_write_parquet(data$Y, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    filename <- paste0(input.hdfs_dir, '/', "pre.atree.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$ATree)){
        spark_write_parquet(data$ATree, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    filename <- paste0(input.hdfs_dir, '/', "pre.events.parquet");
    loginfo(paste("Writing", filename, 'on HDFS'));
    if (!is.null(data$Events)){
        spark_write_parquet(data$Events, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been written because is empty."));
    }
    
    spark_disconnect_all();
}

end_time <- as.numeric(Sys.time())
loginfo(paste("[Writing all files took", paste0("{",end_time - start_time, "s}]")));
loginfo(paste("[Entire Execution took", paste0("{",end_time - overall_start, "s}]")));
loginfo("Pre-process finished correctly.");
