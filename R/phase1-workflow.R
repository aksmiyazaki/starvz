#!/usr/bin/Rscript
library(starvz)
library(tictoc)
library(sparklyr)
library(dplyr)


source("/home/aksmiyazaki/git/starvz/R_package/R/phase1_spark.R")

##############################
# Usage                      #
##############################
usage <- function ()
{
    stop("Usage: pre-workflow.R <directory> [application](optional)\n   where <directory> contains CSV files of the workflow;\n   where [application](optional) is either cholesky or qrmumps.\n\n", call.=FALSE)
}

##############################
# Usage  with spark          #
##############################
spark_usage <- function()
{
    stop("Usage: pre-workflow.R -D <hdfs_directory> [spark_master][application](optional) [spark_home](optional)
        \n   where -D means distributed over spark/hadoop execution;
        \n   where <hdfs_directory> is the HDFS directory which contains large CSV files of the workflow [paje.state.csv];
        \n   where <local_directory> is the local directory which contains small CSV files of the workflow [entities.csv];
        \n   where [spark_master] is the MASTER parameter of a sparklyr connection;
        \n   where [application](optional) is either cholesky or qrmumps;
        \n   where [spark_home](optional) is the location of spark (only needed if SPARK_HOME environment variable isn't set);
        \n   where -V (optional) means VERBOSE logs;
        \n   where -TM (optional) means TIME MEASUREMENTS on.", call.=FALSE)
}

applicationMode <- function() {
    list(sequential = "Sequential", distributed = "Distributed")
}

debugging <- TRUE;

# Get the arguments to this script
if(debugging == FALSE)
{
    args = commandArgs(trailingOnly=TRUE)
    
    if (length(args) < 1){
        usage();
        spark_usage()
    }
    
    if(args[[1]] == '-D'){
        input.mode <- applicationMode()$distributed;
        input.hdfs_dir <- args[[2]];
        if(length(args) > 2)
            input.spark_master <- args[[3]]
        if(length(args) > 3)
            input.application <- args[[4]]
        if(length(args) > 4)
            input.spark_home <- args[[5]]
        
        for(val in args){
            if(val == '-V'){
                input.verbose = TRUE;
            }
            if(val == '-TM'){
                input.time_measure = TRUE;
            }
        }
        
    }else{
        input.mode <- applicationMode$sequential;
        if (length(args) < 2) {
            input.application = "";
        }else{
            input.application = args[[2]];
        }
        input.directory = args[[1]];
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
    input.verbose = TRUE;
    input.time_measure = TRUE;
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
    
    
    
    if(input.mode == applicationMode()$sequential){
        data <- the_reader_function (directory = input.directory,
                                 app_states_fun = states.fun,
                                 state_filter = states.filter,
                                 whichApplication = input.application);
    }else{
        if(input.time_measure) tic('INFO:: Setting up spark environment')
        sc <- setup_spark_env(spark_master = input.spark_master,
                              spark_home = input.spark_home);
        if(input.time_measure) toc();
        
        
    source("/home/aksmiyazaki/git/starvz/R_package/R/phase1_spark.R")
    data <- NULL
    data <- spark_reader_function(sc = sc,
                                  hdfs_directory = input.hdfs_dir,
                                  local_directory = input.local_dir,
                                  app_states_fun = states.fun,
                                  state_filter = states.filter,
                                  whichApplication = input.application);
    
    
    if(input.time_measure) toc()
}

spark_disconnect_all();

loginfo("Let's start to write the pre-processed files as feather data");

# State

loginfo(paste("Writing", filename));
if(input.mode == applicationMode()$sequential)
{
    filename <- "pre.state.feather";
    if (!is.null(data$State)){
        write_feather(data$State, filename);
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }    
}else{
    filename <- paste0(input.hdfs_dir, '/', "pre.state.parquet");
    if (!is.null(data$State)){
        spark_write_parquet(data$State, filename, mode="overwrite");
        loginfo(paste("Data for", filename, "has been successfully wrote."));
    }else{
        loginfo(paste("Data for", filename, "has not been feathered because is empty."));
    }     
}





# Variable
filename <- "pre.variable.feather";
loginfo(filename);
if (!is.null(data$Variable)){
    write_feather(data$Variable, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# Link
filename <- "pre.link.feather";
loginfo(filename);
if (!is.null(data$Link)){
    write_feather(data$Link, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# DAG
filename <- "pre.dag.feather";
loginfo(filename);
if (!is.null(data$DAG)){
    write_feather(data$DAG, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# Y
filename <- "pre.y.feather";
loginfo(filename);
if (!is.null(data$Y)){
    write_feather(data$Y, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# ATree
filename <- "pre.atree.feather";
loginfo(filename);
if (!is.null(data$ATree)){
    write_feather(data$ATree, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# Gaps
filename <- "pre.gaps.feather";
loginfo(filename);
if (!is.null(data$Gaps)){
    write_feather(data$Gaps, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# PMtool
filename <- "pre.pmtool.feather";
loginfo(filename);
if (!is.null(data$pmtool)){
    write_feather(data$pmtool, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

filename <- "pre.pmtool_states.feather";
loginfo(filename);
if (!is.null(data$pmtool_states)){
    write_feather(data$pmtool_states, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# Data Rec
filename <- "pre.data_handles.feather";
loginfo(filename);
if (!is.null(data$data_handles)){
    write_feather(data$data_handles, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

# Tasks Rec
filename <- "pre.tasks.feather";
loginfo(filename);
if (!is.null(data$tasks)){
    write_feather(data$tasks, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

filename <- "pre.task_handles.feather";
loginfo(filename);
if (!is.null(data$task_handles)){
    write_feather(data$task_handles, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

filename <- "pre.events.feather";
loginfo(filename);
if (!is.null(data$Events)){
    write_feather(data$Events, filename);
}else{
    loginfo(paste("Data for", filename, "has not been feathered because is empty."));
}

loginfo("Pre-process finished correctly.");
