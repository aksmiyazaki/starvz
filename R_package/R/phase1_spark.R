setup_spark_env <- function(spark_home=NULL,
                            spark_master = 'yarn')
{
  spark_home_set(path=spark_home)
  sc <- spark_connect(master=spark_master)

  # Logging configuration
  basicConfig();
  logForOrg <- function(record) { paste(record$levelname, record$logger, record$msg, sep=':') }
  addHandler(writeToConsole, formatter=logForOrg);
  removeHandler('basic.stdout');
  loginfo("********** Setting up SPARK environment ***************");
  loginfo(paste("** HOME [", spark_home_dir(), "] MASTER [", spark_master, "] **"));

  if(spark_connection_is_open(sc) == FALSE){
    logerror("Can't Setup Connection, check provided parameters")
    stopifnot(spark_connection_is_open(sc));
  }
  return(sc);
}

sp_read_state_csv <- function (sc = NULL,
                               where = ".",
                               app_states_fun = NULL,
                               state_filter = 0,
                               whichApplication = NULL)
{
    # Check obligatory parameters
    if(is.null(sc)) stop("Spark connection must be set.")
    if(is.null(whichApplication)) stop("whichApplication is NULL, it should be provided");
    if(is.null(app_states_fun)) stop("app_states_fun should be provided to read_state_csv");
    if(!is.data.frame(app_states_fun())) stop("app_state_df is not a data frame");
    if(!all((app_states_fun() %>% names) == c("Kernel", "Color"))) stop("Expecting that app_states_fun returns a dataframe with two columns: Kernel, Color")

    df_states <- copy_to(sc, (app_states_fun() %>% rename(Value = Kernel)))
    state.csv = paste0(where, "/paje.state.csv");

    loginfo(paste("Trying to read state CSV in [", state.csv, "]"));
    try(dfw <- spark_read_csv(sc,
                             "States",
                             state.csv,
                             header = TRUE,
                             delimiter = ',',
                             null_value = NA,
                             charset = 'us-ascii',
                             options=list(ignoreLeadingWhiteSpace = TRUE, ignoreTrailingWhiteSpace = TRUE)),
                            silent = TRUE);

    loginfo(paste("Read state CSV in [", state.csv, "]"));

    loginfo(paste("Starting CSV Manipulation [", state.csv, "]"));

    if ((dfw %>% sdf_nrow) == 0) stop("After reading states, number of rows is zero.");

    # TODO: QRMumps: fix qrmumps kernels names so we have a clean color definition
    if (whichApplication == "qrmumps"){
      stop('NOT IMPLEMENTED APPLICATION QRMUMPS')
    #    dfw <- dfw %>%
    #        mutate(Value = gsub("_perf.*", "", Value)) %>%
    #        mutate(Value = gsub("qrm_", "", Value));
    #    loginfo("Fix of qrmumps state naming completed.");
    }

    # Split application and starpu behavior
    # state_filter:
    # 0 = Based on Runtime fixed States
    # 1 = Based on stricted application name states
    # 2 = Based on non-stricted application name states
    if (state_filter == 0){
        loginfo("Selecting application states based on runtime states.");
        dfw <- dfw %>% mutate(Application = case_when( .$Value %in% all_starpu_states() ~ FALSE, TRUE ~ TRUE));
    }else if (state_filter == 1){
        loginfo("Selecting application states based on custom application stricted states names.");
        # If strict, states need to be exact
        dfw <- dfw %>% mutate(Application = case_when(.$Value %in% (app_state_df %>% .$Kernel) ~ TRUE, TRUE ~ FALSE));
    }else if (state_filter == 2){
        loginfo("Selecting application states based on custom application non-stricted states names.");
        # If not strict, we mark using app_state_df() Kernel field as RE
        state_condition = paste((app_states_fun() %>% .$Kernel), collapse='|');
        dfw <- dfw %>% mutate(Application = case_when(rlike(Value, state_condition) ~ TRUE, TRUE ~ FALSE));
    }


    if ((dfw %>% sdf_nrow) == 0) stop("After application states check, number of rows is zero.");

    loginfo("App state filter completed");


    # remove all application states with NA
    # StarPU is dumping two lines per application state (so, fix in R)
    dfw <- dfw %>% filter(Application == FALSE | (Application == TRUE & !is.na(JobId)));

    # Create three new columns (Node, Resource, ResourceType) - This is StarPU-specific
    # But first, check if this is a multi-node trace (if there is a _, it is a multi-node trace)
    # TODO This is a very weak test, should find something else instead
    firstResourceId <- dfw %>% select(ResourceId) %>% distinct %>% sdf_sort(., columns = "ResourceId") %>% na.omit %>% head(n=1) %>% collect;
    loginfo(paste("First Resource ID is", as.character(firstResourceId[1])));

    if (grepl("CUDA|CPU", unlist(strsplit(as.character(firstResourceId[1]), "_"))[2])){
        loginfo("This is multi-node trace");
        # This is the case for multi-node trace
        dfw <- dfw %>%
          mutate(Resource = ResourceId) %>%
          mutate(Node = as.integer(substr(ResourceId, 1, 1))) %>%
          mutate(ResourceType = as.character(regexp_replace(ResourceId, '[0-9_]+', '')))
    }else{
        loginfo("This is a single-node trace...");
        # This is the case for SINGLE node trace
        dfw <- dfw %>%
          mutate(Resource = ResourceId) %>%
          mutate(Node = 0) %>%
          mutate(ResourceType = as.character(regexp_replace(ResourceId, '[0-9_]+', '')))
    }

    loginfo("Node, Resource, ResourceType definition is ready");

    loginfo("Define colors is starting right now.");

    sp_df_colors <- NULL
    # In case application is not specified
    if(whichApplication==""){
      # Get only application states
      dfcolors <- dfw %>% filter(Application == TRUE) %>%
          select(Value) %>%
          unique()

      # Get the number of states to generate colors
      nc <- dfcolors %>% nrow()

      # TODO: Using set1 right now to generate colors, max of 9 states
      c <- brewer.pal(n = nc, name = "Set1")

      # Match States and Colors
      dfcolors <- dfcolors %>% mutate(Color = c) %>%
          arrange(Value, Color) %>%
          mutate(Color = na.locf(Color, na.rm=FALSE)) %>%
          unique;

    }else{

      # Define colors
      dfcolors <- dfw %>%
        select(Value) %>%
        distinct %>%
        mutate(Value = as.character(Value), Color = NA) %>%
        union_all(., df_states) %>%
        collect %>%
        arrange(Value, Color) %>%
        mutate(Color = na.locf(Color, na.rm=FALSE)) %>%
        unique %>%
        drop_na(., Value)

      sp_df_colors <- copy_to(sc, dfcolors, overwrite = TRUE);

      loginfo("Colors data.frame is defined.");
    }

    if(is.null(sp_df_colors)) stop("df_colors is null");

    # Apply
    dfw <- dfw %>% left_join(sp_df_colors, by="Value");

    loginfo("Left joining the colors has completed.");


    # Specific cholesky colors: sufficiently harmless
    if (whichApplication == "cholesky"){
        loginfo("This is a cholesky application, colors are hard-coded");
        dfw <- dfw %>%
            mutate(Color = case_when(
                       rlike(Value, "potrf") ~ "#e41a1c",
                       rlike(Value, "trsm") ~ "#377eb8",
                       rlike(Value, "syrk") ~ "#984ea3",
                       rlike(Value, "gemm") ~ "#4daf4a",
                       rlike(Value, "plgsy") ~ "yellow",
                       TRUE ~ Color));
    }

    loginfo("Cholesky colors completed");

    # Detect outliers
    if (whichApplication == "cholesky"){

      loginfo("Attempt to detect outliers using a basic model.");
      # The below BorderValue definition used to be a function, made in just one dplyr set of operations.
      # Since sparklyr works inneficient with User Defined Functions, this seems to be a suitable approach.
      auxdf <-dfw %>%
        group_by(Value, ResourceType) %>%
        summarise(BorderValue = (percentile_approx(Duration, .75) + (percentile_approx(Duration, .75) - percentile_approx(Duration, .25)) * 1.5)) %>%
        collect %>%
        ungroup();
      sp_aux_df <- copy_to(sc, auxdf, overwrite = TRUE);

      dfw <- dfw %>% left_join(sp_aux_df, by=c("Value", "ResourceType")) %>% mutate(Outlier = ifelse(Duration > BorderValue, TRUE, FALSE)) %>% select(-BorderValue)
    }else{
        loginfo("No outlier detection; use NA in the corresponding column.");
        dfw <- dfw %>% mutate(Outlier = NA);
    }


    loginfo("Define the ZERO timestamp.");


    # Define the global ZERO (to be used with other trace date)
    ZERO <<- dfw %>% filter(Application == TRUE) %>% select(Start) %>% sdf_collect %>% min;

    loginfo(paste("ZERO is ", ZERO));

    # The new zero because of the long initialization phase
    dfw <- dfw %>% mutate(Start = Start - ZERO, End = End - ZERO);

    # Get rid of all observations before ZERO
    dfw <- dfw %>% filter(Start >= 0);

    return(dfw);
    # The problem is that Vinicius traces do not have "Iteration" column
    # We need to create it based on the Tag
    if (data$State %>% filter(Application == TRUE) %>% head(., n=1) %>% collect %>% .$Iteration %>% is.na){
      stop("No examples of absence of Iteration Column, must check.");
        dfw <- dfw %>%
            mutate(Iteration = case_when(
                       rlike(Value, "potrf") ~ as.integer(paste0("0x", substr(Tag, 14, 16))),
                       rlike(Value, "trsm") ~ as.integer(paste0("0x", substr(Tag, 11, 13))),
                       rlike(Value, "syrk") ~ as.integer(paste0("0x", substr(Tag, 8, 10))),
                       rlike(Value, "gemm") ~ as.integer(paste0("0x", substr(Tag, 8, 10))),
                       TRUE ~ as.integer(-10)));
    }

    # QRMumps case:
    # When the trace is from qr_mumps (by Ian), the elimination tree
    # node is encoded in the Tag field, we need to convert it to the
    # appropriate ANode using the following code. We do that for all kind
    # of traces, but the ANode column is only valid for the qr_mump traces.
    if (whichApplication == "qrmumps"){
        stop("Untested application qrmumps.");
        dfw <- dfw %>% mutate(ANode = NA, ANode = as.character(strtoi(as.integer(paste0("0x", substr(Tag, 9, 16))))));
    }

    return(dfw);
}

sp_read_vars_set_new_zero <- function (sc = NULL, where = ".")
{
    variable.csv = paste0(where, "/paje.variable.csv");
    loginfo(paste("Starting read of [",variable.csv,"]"));

    try(dfv <- spark_read_csv(sc,
                              "Variables",
                              variable.csv,
                              header = TRUE,
                              delimiter = ',',
                              null_value = NA,
                              charset = 'us-ascii',
                              options=list(ignoreLeadingWhiteSpace = TRUE, ignoreTrailingWhiteSpace = TRUE)),
        silent = TRUE);

    if((dfv %>% sdf_nrow) > 0){
      loginfo(paste("Read of", variable.csv, "completed"));
    }else{
      stop(paste("Files", variable.feather, "or", variable.csv, "do not exist"));
    }

    dfv <- dfv %>%
      # the new zero because of the long initialization phase
      mutate(Start = Start - ZERO, End = End - ZERO) %>%
      # filter all variables during negative timings
      filter(Start >= 0, End >= 0) %>%
      # create three new columns (Node, Resource, ResourceType)
      # This is StarPU-specific
      mutate(Node = as.integer(substr(ResourceId, 1, 1))) %>%
      mutate(ResourceType = as.character(regexp_replace(ResourceId, '[0-9_]+', ''))) %>%
      # manually rename variables names
      # Attention: first, the beautiful 4 backslash escape to special characters and the mutate chain
      # For some reason, the multi parameter mutate was returning weird results.
      mutate (Type = regexp_replace(Type, "^Number of Ready Tasks", "Ready")) %>%
      mutate (Type = regexp_replace(Type, "^Number of Submitted Uncompleted Tasks", "Submitted")) %>%
      mutate (Type = regexp_replace(Type, "^Bandwidth In \\\\(MB\\\\/s\\\\)", "B. In (MB/s)")) %>%
      mutate (Type = regexp_replace(Type, "^Bandwidth Out \\\\(MB\\\\/s\\\\)", "B. Out (MB/s)"))

    return(dfv);
}

sp_atree_load <- function(sc = NULL, where = "."){
    atree.csv = paste0(where, "/atree.csv");
    df <- NULL;

    loginfo(paste0("Trying to get atree.csv in [", atree.csv, "]"));
    try(df <- spark_read_csv(sc, "atree", atree.csv, header = TRUE, charset = 'us-ascii'), silent = TRUE)

    if(is.null(df)){
        loginfo(paste("File", atree.csv, "do not exist."));
        return(NULL);
    }else{
      loginfo("TODO: not implemented.")
    }

    return(df);
}

# This function gets a data.tree object and calculate three properties
# H: height, P: position, D: depth
atree_coordinates <- function (atree, height = 1)
{
    defineHeightPosition <- function (node, curPos, depth)
    {
        if (length(node$children) == 0){
            # My coordinates are the same of the parents
            node$H = node$parent$H;
            node$P = node$parent$P;
            node$D = node$parent$D;
        }else{
            # Defined this node properties
            node$H = height;
            node$P = curPos;
            node$D = depth;
            # Before recursion, set the new Y position at curPos
            curPos = curPos + node$H;
            # Recurse
            for (child in node$children){
                curPos = defineHeightPosition (child, curPos, (depth+1));
            }
        }
        return(curPos);
    }
    defineHeightPosition(atree, 0, 0);
    return(atree);
}

# This function gets a data.tree object and converts it to a tibble
# Expects three properties on each node (H, P, and D), as defined above
atree_to_df <- function (node)
{
    ndf <- tibble(
        Parent = ifelse(is.null(node$parent), NA, node$parent$name),
        ANode = node$name,
        Height = node$H,
        Position = node$P,
        Depth = node$D);
    for (child in node$children) ndf <- ndf %>% bind_rows(atree_to_df(child));
    return(ndf);
}

spark_reader_function <- function (sc = NULL, hdfs_directory = ".", local_directory = ".", app_states_fun = NULL, state_filter = 0, whichApplication = NULL)
{
    # Start of reading procedure
    if(is.null(app_states_fun)) stop("app_states_fun is obligatory for reading");
    if(is.null(sc)) stop("spark connection is mandatory for reading");

    # Read the elimination tree
    dfa <- sp_atree_load(sc = sc,
                         where = hdfs_directory);

    # Read states

    dfw <- sp_read_state_csv (sc = sc,
                              where = hdfs_directory,
                              app_states_fun=app_states_fun,
                              state_filter=state_filter,
                              whichApplication = whichApplication) %>%
            sp_hl_y_coordinates(where = local_directory);



    # QRMumps case:
    # If the ATree is available and loaded, we create new columns for each task
    # to hold Y coordinates for the temporal elimination tree plot
    if (!is.null(dfa)){
        stop('NOT IMPLEMENTED')
        dfap <- dfa %>% select(-Parent, -Depth) %>% rename(Height.ANode = Height, Position.ANode = Position);
        dfw <- dfw %>% left_join(dfap, by="ANode");
        dfap <- NULL;
    }

    if((dfw %>% sdf_nrow) == 0) stop("After reading states, number of rows is zero.");

    # Read variables
    dfv <- sp_read_vars_set_new_zero(sc = sc, where = hdfs_directory);

    # Read links
    dfl <- sp_read_links (sc = sc, where = hdfs_directory);

    # Read DAG
    dfdag <- sp_read_dag (sc = sc,
                          where = hdfs_directory, dfw, dfl);

    if (is.null(dfdag)){
        stop('DAG is NULL');
        #TODO: Adapt Vinicius DAG for sparklyr
        # If dag is not available, try Vinicius DAG
        dagVinCSV <- paste0(directory, "/dag_vinicius.csv");
        loginfo(paste("Reading DAG", dagVinCSV));

        # Check if this a DAG from Vinicius
        if (file_can_be_read(dagVinCSV)){
            dfdag <- read_csv(dagVinCSV, trim_ws=TRUE, col_names=TRUE) %>%
                mutate(Dependent = strsplit(Dependent, " ")) %>%
                unnest(Dependent) %>%
                merge(., (dfw), #%>% filter(Application == TRUE))
                      by.x="JobId", by.y="JobId", all=TRUE) %>%
                mutate(Cost = ifelse(is.na(Duration), 0, -Duration)) %>%
                as_tibble();
        }else{
            dfdag <- NULL;
        }
    }

    # Read entities.csv and register the hierarchy (with Y coordinates)
    # Note that now it copies to a spark dataframe.
    dfhie <- copy_to(sc, hl_y_paje_tree (where = local_directory), overwrite = TRUE);

    # PMTools information
    #dpmtb <- pmtools_bounds_csv_parser (where = directory);
    dpmtb <- NULL

    #dpmts <- pmtools_states_csv_parser (where = directory, whichApplication = whichApplication, Y=dfhie, States = dfw);
    dpmts <- NULL


    # Data.rec
    #ddh <- data_handles_csv_parser (where = directory);
    ddh <- NULL

    # Tasks.rec
    #dtasks <- tasks_csv_parser (where = directory);
    dtasks <- NULL

    loginfo("Assembling the named list with the data from this case.");

    # Events
    devents <- sp_events_csv_parser (sc=sc,
                                  where = hdfs_directory);

    data <- list(DistributedOrigin=hdfs_directory, LocalOrigin=local_directory, State=dfw, Variable=dfv, Link=dfl, DAG=dfdag, Y=dfhie, ATree=dfa,
                 pmtool=dpmtb, pmtool_states=dpmts, data_handles=ddh, tasks=dtasks$tasks, task_handles=dtasks$handles, Events=devents);



    # Calculate the GAPS from the DAG
    if (whichApplication == "cholesky"){
        data$Gaps <- sp_gaps(data);
    }else{
        data$Gaps <- NULL;
    }
    return(data);

}

hl_y_paje_tree <- function (where = ".")
{
  entities.feather = paste0(where, "/entities.feather");
  entities.csv = paste0(where, "/entities.csv");

  if (file.exists(entities.feather)){
    loginfo(paste("Reading ", entities.feather));
    dfe <- read_feather(entities.feather);
    loginfo(paste("Read of", entities.feather, "completed"));
  }else if (file.exists(entities.csv)){
    loginfo(paste("Reading ", entities.csv));
    dfe <- read_csv(entities.csv,
                    trim_ws=TRUE,
                    col_types=cols(
                      Parent = col_character(),
                      Name = col_character(),
                      Type = col_character(),
                      Nature = col_character()
                    ));
    loginfo(paste("Read of", entities.csv, "completed"));
  }else{
    loginfo(paste("Files", entities.feather, "or", entities.csv, "do not exist."));
    return(NULL);
  }

  # first part: read entities, calculate Y
  # If this file is read with readr's read_csv function, the data.tree does not like

  if ((dfe %>% nrow) == 0) stop(paste("After reading the entities file, the number of rows is zero"));

  workertree <- tree_filtering (dfe,
                                c("Link", "Event", "Variable"),
                                c("GFlops", "Memory Manager", "Scheduler State", "User Thread", "Thread State"));

  # Customize heigth of each object
  dfheights = data.frame(Type = c("Worker State", "Communication Thread State"), Height = c(1, 1));
  # Customize padding between containers
  dfpaddings = data.frame(Type = c("MPI Program"), Padding = c(2));

  # Calculate the Y coordinates with what has left
  workertree <- y_coordinates(workertree, dfheights, dfpaddings);

  #print(workertree, "Type", "Nature", "H", "P", limit=200);
  # Convert back to data frame
  workertreedf <- dt_to_df (workertree);

  if ((workertreedf %>% nrow) == 0) stop("After converting the tree back to DF, number of rows is zero.");

  return(workertreedf);
}

pmtools_bounds_csv_parser <- function (where = ".")
{
    entities.feather = paste0(where, "/pmtool.feather");
    entities.csv = paste0(where, "/pmtool.csv");

    if (file.exists(entities.feather)){
        loginfo(paste("Reading ", entities.feather));
        pm <- read_feather(entities.feather);
        loginfo(paste("Read of", entities.feather, "completed"));
    }else if (file.exists(entities.csv)){
        loginfo(paste("Reading ", entities.csv));
        pm <- read_csv(entities.csv,
                        trim_ws=TRUE,
                        col_types=cols(
                            Alg = col_character(),
                            Time = col_double()
                        ));
        # pmtools gives time in microsecounds
        pm[[2]] <- pm[[2]]/1000
        loginfo(paste("Read of", entities.csv, "completed"));
    }else{
        loginfo(paste("Files", entities.feather, "or", entities.csv, "do not exist."));
        return(NULL);
    }
    ret <- pm;
    return(ret);
}

pmtools_states_csv_parser <- function (where = ".", whichApplication = NULL, Y = NULL, States = NULL)
{
    entities.feather = paste0(where, "/pmtool_states.feather");
    entities.csv = paste0(where, "/pmtool_states.csv");

    if (file.exists(entities.feather)){
        loginfo(paste("Reading ", entities.feather));
        pm <- read_feather(entities.feather);
        loginfo(paste("Read of", entities.feather, "completed"));
    }else if (file.exists(entities.csv)){
        loginfo(paste("Reading ", entities.csv));

        #sched Tid   worker taskType JobId start duration end

        pm <- read_csv(entities.csv,
                        trim_ws=TRUE,
                        col_types=cols(
                            sched = col_character(),
                            Tid = col_integer(),
                            worker = col_integer(),
                            taskType = col_character(),
                            JobId = col_character(),
                            start = col_double(),
                            duration = col_double(),
                            end = col_double()
                        ));
        #pmtools states gives time in milisecounds

        pm[[6]] <- pm[[6]]/1000
        pm[[7]] <- pm[[7]]/1000
        pm[[8]] <- pm[[8]]/1000

        names(pm)[names(pm) == 'taskType'] <- 'Value'
        names(pm)[names(pm) == 'start'] <- 'Start'
        names(pm)[names(pm) == 'end'] <- 'End'
        names(pm)[names(pm) == 'duration'] <- 'Duration'
        names(pm)[names(pm) == 'worker'] <- 'ResourceId'

        pm <- separate(data = pm, col = JobId, into = c("JobId", "Tag"), sep = "\\:")

        fileName <- "platform_file.rec"
        conn <- file(fileName,open="r")
        linn <-readLines(conn)

        devices <- c()

        for (i in 1:length(linn)){
        	if(substr(linn[i], 1, 18)=="%rec: worker_count"){
        		for (y in i:length(linn)){
        			if(substr(linn[y], 1, 12)=="%rec: timing"){
        				break;
        			}
        			if(substr(linn[y], 1, 14)=="Architecture: "){
                hard <- substr(linn[y], 15, nchar(linn[y]))
                if(substr(hard,1,3)=="cpu"){
                  hard <- "CPU"
                }else{
                  hard <- paste0(toupper(hard),"_")
                }
        				y <- y + 1
        				i <- i + 1
                num <- as.numeric(substr(linn[y], 12, nchar(linn[y])))
                for(z in 1:num){
                  devices <- c(devices, paste0(hard, z-1))
                }
        			}
        		}
        	}else if(substr(linn[i], 1, 12)=="%rec: timing"){
        		break;
        	}
        }

        pm[[3]] <- devices[pm[[3]]+1]

        pm <- pm %>% left_join((Y %>% select(-Type, -Nature)), by=c("ResourceId" = "Parent"))
        #print(States)
        #print(pm)
        pm <- pm %>% left_join((States %>% select(Iteration, JobId)), by=c("JobId" = "JobId"))

        if (whichApplication == "cholesky"){
            pm <- pm %>%
                mutate(Color = case_when(
                           Value=="dpotrf" ~ "#e41a1c",
                           Value=="dtrsm" ~ "#377eb8",
                           Value=="dsyrk" ~ "#984ea3",
                           Value=="dgemm" ~ "#4daf4a",
                           Value=="dplgsy" ~ "yellow",
                           TRUE ~ "#000"));
        }

        #print(pm)
        loginfo(paste("Read of", entities.csv, "completed"));
    }else{
        loginfo(paste("Files", entities.feather, "or", entities.csv, "do not exist."));
        return(NULL);
    }
    ret <- pm;
    return(ret);
}

data_handles_csv_parser <- function (where = ".")
{
    entities.feather = paste0(where, "/data_handles.feather");
    entities.csv = paste0(where, "/rec.data_handles.csv");

    if (file.exists(entities.feather)){
        loginfo(paste("Reading ", entities.feather));
        pm <- read_feather(entities.feather);
        loginfo(paste("Read of", entities.feather, "completed"));
    }else if (file.exists(entities.csv)){
        loginfo(paste("Reading ", entities.csv));
        pm <- read_csv(entities.csv,
                        trim_ws=TRUE,
                        col_types=cols(
                            Handle = col_character(),
                            HomeNode = col_integer(),
                            Size = col_integer(),
                            Coordinates = col_character()
                        ));

        # Not supported in feather
        # pm$Coordinates <- lapply(strsplit(pm$Coordinates, " "), as.integer);
        loginfo(paste("Read of", entities.csv, "completed"));
    }else{
        loginfo(paste("Files", entities.feather, "or", entities.csv, "do not exist."));
        return(NULL);
    }
    ret <- pm;

    return(ret);
}

task_handles_parser <- function (where = ".")
{
    entities.feather = paste0(where, "/task_handles.feather");

    if (file.exists(entities.feather)){
        loginfo(paste("Reading ", entities.feather));
        ret <- read_feather(entities.feather);
        loginfo(paste("Read of", entities.feather, "completed"));
        return(ret);
    }

    return(NULL);
}

tasks_csv_parser <- function (where = ".")
{
    entities.feather = paste0(where, "/tasks.feather");
    entities.csv = paste0(where, "/rec.tasks.csv");

    task_handles <- task_handles_parser(where = where);

    if (file.exists(entities.feather)){
        loginfo(paste("Reading ", entities.feather));
        pm <- read_feather(entities.feather);
        loginfo(paste("Read of", entities.feather, "completed"));
    }else if (file.exists(entities.csv) & file.info(entities.csv)$size > 0){
        loginfo(paste("Reading ", entities.csv));
        pm <- read_csv(entities.csv,
                        trim_ws=TRUE,
                        col_types=cols(
                            Control = col_character(),
                            JobId = col_character(),
                            SubmitOrder = col_integer(),
                            SubmitTime = col_double(),
                            Handles = col_character(),
                            MPIRank = col_integer(),
                            DependsOn = col_character(),
                            Tag = col_character(),
                            Footprint = col_character(),
                            Iteration = col_integer(),
                            Name = col_character(),
                            Model = col_character(),
                            Priority = col_integer(),
                            WorkerId = col_integer(),
                            MemoryNode = col_integer(),
                            StartTime = col_double(),
                            EndTime = col_double(),
                            Parameters = col_character(),
                            Modes = col_character(),
                            Sizes = col_character()
                        ));
        # sort the data by the submit order
        pm <- pm[with(pm, order(SubmitOrder)), ]
        # Tasks have multiple handles, get them in a different structure
        handles_dep = pm %>% select(JobId) %>% mutate(Handles = strsplit(pm$Handles, " "),
                                Modes = strsplit(pm$Modes, " "),
                                Sizes = lapply(strsplit(pm$Sizes, " "), as.integer))
        # unnest the lists
        task_handles <- unnest(handles_dep);

        # We will save the task_handle structre, we can remove these columns
        pm <- pm %>% select(-Handles, -Modes, -Sizes)

        loginfo(paste("Read of", entities.csv, "completed"));
    }else{
        loginfo(paste("Files", entities.feather, "or", entities.csv, "do not exist."));
        return(NULL);
    }

    return( list(tasks = pm, handles=task_handles) );
}

sp_events_csv_parser <- function (sc = NULL, where = ".")
{
    events.csv = paste0(where, "/paje.events.csv");
    loginfo(paste("Reading and parsing ", events.csv));

    try(pm <- spark_read_csv(sc,
                              "Events",
                              events.csv,
                              header = TRUE,
                              delimiter = ',',
                              null_value = NA,
                              charset = 'us-ascii',
                              options=list(ignoreLeadingWhiteSpace = TRUE, ignoreTrailingWhiteSpace = TRUE)),
                silent = TRUE);

    if(is.null(pm) || ((pm %>% sdf_nrow) <= 0)){
      stop(paste(events.csv), " not found.");
    }


    # sort the data by the start time
    pm <- pm %>%
          sdf_sort(c('Start'));

    # set the new zero because of the long initialization phase
    pm <- pm %>%
        mutate(Start = Start - ZERO);

    loginfo(paste("Read of", events.csv, "completed"));
    return(pm);
}

sp_hl_y_coordinates <- function (dfw = NULL, where = ".")
{
  loginfo("Preparing to load coordinates");
  if (is.null(dfw)) stop("The input data frame with states is NULL");

  # first part: read entities, calculate Y
  workertreedf <- hl_y_paje_tree (where);
  # sparkly part - turns df into a spark df
  aux_df <- copy_to(sc, workertreedf, overwrite = TRUE);

  # second part: left join with Y
  dfw <- dfw %>>%
      # the left join to get new Y coordinates
      left_join (aux_df, by=c("ResourceId" = "Parent", "Type" = "Type", "Nature" = "Nature"));

  return(dfw);
}

tree_filtering <- function (dfe, natures, types)
{
  loginfo("Starting the tree filtering to create Y coordinates");

  dfe %>%
    # Mutate things to character since data.tree don't like factors
    mutate (Type = as.character(Type), Nature = as.character(Nature)) %>%
    # Filter things I can't filter using Prune (because Prune doesn't like grepl)
    # Note that this might be pottentially dangerous and works only for StarPU traces
    filter (!grepl("InCtx", Parent), !grepl("InCtx", Name)) %>%
    # Rename the reserved word root
    mutate (Name = gsub("root", "ROOT", Name),
            Parent = gsub("root", "ROOT", Parent)) %>%
    # Remove a node named 0 whose parent is also named 0
    filter (Name != 0 & Parent != 0) %>%
    # Convert to data.frame to avoid compatibility problems between tibble and data.tree
    as.data.frame() %>>%
    # Remove all variables
    #filter (Nature != "Variable") %>%
    # Remove bogus scheduler (should use a new trace)
    #filter (Name != "scheduler") %>%
    # Convert to data.tree object
    as.Node(mode="network") -> tree;
  # Remove all nodes that are present in the natures list
  if (!is.null(natures)){
    tree <- tree %>>% (~ Prune(., function(node) !(node$Nature %in% natures)) );
  }
  # Remove all types that are present in the types list
  if (!is.null(types)){
    tree <- tree %>>% (~ Prune(., function(node) !(node$Type %in% types)) );
  }

  loginfo("Tree filtering completed.");

  return(tree);
}

y_coordinates <- function (atree, heights, paddings)
{
    loginfo ("Starting y_coordinates");
    defineHeightPosition <- function (node, dfhs, dfps, curPos)
    {
        node$P = curPos;
        if(!is.null(node$Nature) && node$Nature == "State"){
            node$H = dfhs %>% filter(Type == node$Type) %>% .$Height;
            # This is a StarPU+MPI hack to make CUDA resources look larger
            if (grepl("CUDA", node$parent$name)) node$H = node$H * 2;
            curPos = curPos + node$H;
        }else{
            padding = 0;
            if (!is.null(node$Type)){
                padding = dfps %>% filter(Type == node$Type);
                if (nrow(padding) == 0){
                    padding = 0;
                }else{
                    padding = padding %>% .$Padding;
                }
            }

            for (child in node$children){
                curPos = defineHeightPosition (child, dfhs, dfps, (curPos+padding));

            }
            if(length(node$children)){
                node$H = sum(sapply(node$children, function(child) child$H));
            }else{
                node$H = 0;
            }
        }
        return(curPos);
    }

    atree$Set(H = NULL);
    atree$Set(P = NULL);
    defineHeightPosition(atree, heights, paddings, 0);
    loginfo ("The call for y_coordinates has completed.");
    return(atree);
}

dt_to_df <- function (node)
{
    loginfo ("Converting data.tree to data.frame");
    ret <- dt_to_df_inner (node);
    loginfo ("Conversion from data.tree to data.frame has completed.");
    return(ret);
}

dt_to_df_inner <- function (node)
{
    cdf <- data.frame();
    ndf <- data.frame();
    if(!is.null(node$Nature) && node$Nature == "State"){
        ndf <- data.frame(
            Parent = node$parent$name,
            Type = node$name,
            Nature = node$Nature,
            Height = node$H,
            Position = node$P);
    }else{
        for (child in node$children){
            cdf <- rbind(cdf, dt_to_df_inner(child));
        }
    }
    ret <- rbind(ndf, cdf);
    return(ret);
}

sp_gaps.f_backward <- function (data)
{
    # Create the seed chain
    tmpdag <- data$DAG %>%
              filter(rlike(JobId, "mpicom"))

    if(is.null(tmpdag) || (tmpdag %>% sdf_nrow) == 0)
      tmpdag <- data$DAG

    seedchain <- tmpdag %>%
                  rename(DepChain = JobId, Member = Dependent) %>%
                  select(DepChain, Member);

    f2 <- function (dfdag, chain.i)
    {
        full.i <-
          dfdag %>%
          select(JobId, Dependent, Application, Value);

        full.o <- full.i %>%
                  left_join(chain.i, by=c("JobId" = "Member"));

        # If there are no application tasks in dependency chains, keep looking
        if ((full.o %>% filter(!is.na(DepChain), Application == TRUE) %>% sdf_nrow) == 0) {
            # Prepare the new chain
            chain.o <- full.o %>%
                filter(!is.na(DepChain)) %>%
                rename(Member = Dependent) %>%
                select(DepChain, Member);
            return(f2(full.o, chain.o));
        }else{
            return(full.o);
        }
    }
    return(f2(data$DAG, seedchain));
}

sp_gaps.f_forward <- function (data)
{
    # Create the seed chain
    tmpdag <- data$DAG %>%
      filter(rlike(JobId, "mpicom"))

    if(is.null(tmpdag) || (tmpdag %>% sdf_nrow) == 0)
      tmpdag <- data$DAG

    seedchain <- tmpdag %>%
                  rename(DepChain = Dependent, Member = JobId) %>%
                  select(DepChain, Member);

    f2 <- function (dfdag, chain.i)
    {
      full.i <- dfdag %>%
                select(JobId, Dependent, Application, Value);

      full.o <- full.i %>%
                left_join(chain.i, by=c("Dependent" = "Member"));

      # If there are no application tasks in dependency chains, keep looking
      if ((full.o %>% filter(!is.na(DepChain), Application == TRUE) %>% sdf_nrow) == 0) {
          # Prepare the new chain
          chain.o <- full.o %>%
              filter(!is.na(DepChain)) %>%
              rename(Member = JobId) %>%
              select(DepChain, Member);
          return(f2(full.o, chain.o));
      }else{
          return(full.o);
      }
  }
  return(f2(data$DAG, seedchain));
}

sp_gaps <- function (data)
{
  loginfo("Starting the gaps calculation.");

  if(is.null(data$DAG)) return(NULL);
  if(is.null(data$State)) return(NULL);

  data.b <- sp_gaps.f_backward(data) %>%
            filter(!is.na(DepChain)) %>%
            select(JobId, DepChain) %>%
            rename(Dependent = JobId) %>%
            rename(JobId = DepChain) %>%
            select(JobId, Dependent) %>%
            distinct;

  loginfo("backward completed");

  data.f <- sp_gaps.f_forward(data) %>%
            filter(!is.na(DepChain)) %>%
            select(JobId, DepChain) %>%
            rename(Dependent = DepChain) %>%
            select(JobId, Dependent) %>%
            distinct;

  loginfo("forward completed");

  data.z <- data$DAG %>%
            filter(Application == TRUE) %>%
            select(JobId, Dependent)

  loginfo("z completed");

  loginfo("The gaps calculation has completed.");

  # Create the new gaps DAG
  dfw <- data$State %>%
      filter(Application == TRUE) %>%
      select(JobId, Value, ResourceId, Node, Start, End);
  if(is.null(data$Link)){
      dfl <- data.frame()
      data.b.dag <- data.frame()
      data.f.dag <- data.frame()
  } else {
      dfl <- data$Link %>%
          filter(rlike(Key, "mpicom")) %>%
          mutate(Value = NA, ResourceId = Origin, Node = NA) %>%
          rename(JobId = Key) %>%
          select(JobId, Value, ResourceId, Node, Start, End);

      loginfo("Joining on B");
      data.b %>%
          left_join(dfl, by=c("JobId" = "JobId"), suffix=c("_x", "_y")) %>%
          left_join(dfw, by=c("Dependent" = "JobId"), suffix=c("_x", "_y")) -> data.b.dag;
      loginfo("Joining on F");
      data.f %>%
          left_join(dfw, by=c("JobId" = "JobId")) %>%
          left_join(dfl, by=c("Dependent" = "JobId"), suffix=c("_x", "_y")) -> data.f.dag;
  }
  loginfo("Joining on Z");
  data.z %>%
      left_join(dfw, by=c("JobId" = "JobId"), suffix=c("_x", "_y")) %>%
      left_join(dfw, by=c("Dependent" = "JobId"), suffix=c("_x", "_y")) -> data.z.dag;

  return(sdf_bind_rows(data.z.dag, data.b.dag, data.f.dag));
}

sp_read_links <- function (sc = NULL, where = ".")
{
    link.csv = paste0(where, "/paje.link.csv");
    loginfo(paste("Reading ", link.csv));

    try(dfl <- spark_read_csv(sc,
                              "Link",
                              link.csv,
                              header = TRUE,
                              delimiter = ',',
                              null_value = NA,
                              charset = 'us-ascii',
                              options=list(ignoreLeadingWhiteSpace = TRUE, ignoreTrailingWhiteSpace = TRUE)),
        silent = TRUE);

    #TODO: verify behavior with missing file in hdfs.
    loginfo(paste("Read of", link.csv, "completed"));

    # Check if number of lines is greater than zero
    if ((dfl %>% sdf_nrow) == 0){
        logwarn("After attempt to read links, number of rows is zero");
        return(NULL);
    }

    # Read links
    dfl <- dfl %>%
        # the new zero because of the long initialization phase
        mutate(Start = Start - ZERO, End = End - ZERO) %>%
        # filter all variables during negative timings
        filter(Start >= 0, End >= 0);

    return(dfl);
}

sp_read_dag <- function (sc = NULL, where = ".", dfw = NULL, dfl = NULL)
{
    dag.csv = paste0(where, "/dag.csv");
    loginfo(paste("Reading ", dag.csv));

    try(dfdag <- spark_read_csv(sc,
                              "DAG",
                              dag.csv,
                              header = TRUE,
                              delimiter = ',',
                              null_value = NA,
                              charset = 'us-ascii',
                              options=list(ignoreLeadingWhiteSpace = TRUE, ignoreTrailingWhiteSpace = TRUE)),
        silent = TRUE);

    loginfo(paste("Read of", dag.csv, "completed"));

    # Read the DAG in the CSV format, do some clean-ups
    dfdag <- dfdag %>%
        # Put in the right order
        select(JobId, Dependent) %>%
        # Communication task ids have too much information, clean-up both columns (JobId, Dependent)
        mutate(JobId = regexp_replace(JobId, "mpi_.*_", "mpicom_")) %>%
        mutate(Dependent = regexp_replace(Dependent, "mpi_.*_", "mpicom_"));

    # Check dfw existence
    stopifnot(!is.null(dfw));

    loginfo("Merge state data with the DAG");

    # Do the two merges (states and links)
    dfdags <- dfdag %>%
        # Get only non-MPI tasks JobIds
        filter(!rlike(JobId, "mpicom")) %>%
        # Merge task information from the trace (states: dfw)
        full_join((dfw %>% filter(Application == TRUE)), by="JobId");

    loginfo("Merge state data with the DAG completed");

    # Check dfl existence
    if (!is.null(dfl)){
        loginfo("Get MPI tasks (links) to enrich the DAG");

        dfdagl <- dfdag %>%
            # Get only MPI tasks JobIds
            filter(rlike(JobId, "mpicom"))  %>%
            # Merge MPI communicaton task information from the trace (links: dfl)
            full_join(dfl, by=c("JobId" = "Key")) %>%
            # Align columns with state-based tasks
            # 1. Remove columns
            select(-Container, -Origin) %>%
            # 2. Change types
            mutate(Size = as.character(Size)) %>%
            # 3. Dest becomes ResourceId for these MPI tasks
            rename(ResourceId = Dest) %>%
            mutate(Node = as.integer(substr(ResourceId, 1, 1))) %>%
            mutate(ResourceType = as.character(regexp_replace(ResourceId, '[0-9_]+', '')));

        dfdag <- dfdags %>% sdf_bind_rows(dfdagl);

        loginfo("Merge link data with the DAG completed");
    }else{
        dfdag <- dfdags;
    }

    # Finally, bind everything together, calculate cost to CPB
    dfdag <- dfdag %>%
      # Calculate the cost as the inverse of the duration (so boost's CPB code can work)
      mutate(Cost = ifelse(is.na(Duration), 0, -Duration))

    return(dfdag);
}

starpu_states <- function()
{
    c("Callback", "FetchingInput", "Idle", "Initializing", "Overhead", "PushingOutput", "Scheduling", "Submitting task", "Progressing", "Sleeping", "Submiting task", "Waiting all tasks", "Building task", "Deinitializing");
}

all_starpu_states <- function()
{
    c("Callback", "FetchingInput", "Idle", "Initializing", "Overhead", "PushingOutput", "Scheduling", "Submitting task", "Progressing", "Sleeping", "Submiting task", "Waiting all tasks", "Building task", "Deinitializing", "Executing");
}

cholesky_states <- function()
{
    cholesky_colors() %>% .$Kernel;
}

scalfmm_states <- function()
{
    scalfmm_colors() %>% .$Kernel;
}

cholesky_colors <- function()
{
    tibble(
        Kernel = c("potrf", "trsm", "syrk", "gemm", "plgsy"),
        Color = c("#e41a1c", "#377eb8", "#984ea3", "#4daf4a", "yellow"));
}

cfd_colors <- function()
{
    tibble(
        Kernel = c("fluid_bound", "diffuse_1", "diffuse_relax", "macCormack_commit", "macCormack_2", "macCormack_1", "obstacle_boundary_1", "conserve_1", "conserve_relax", "conserve_commit", "obstacle_velocity", "initial_state"),
        Color = c("#e41a1c", "#377eb8", "#984ea3", "#9a4ea3", "#4daf4a", "#ffff33", "#a65628", "#f781bf", "#ea1a1c", "#37beb8", "#4eaf4a", "#ff7f00"));
}

scalfmm_colors <- function()
{
    tibble(
# For the trace I've been given
        Kernel = c("L2L-level", "L2P",     "M2L-level", "M2L-out-level", "M2M",     "P2M",     "P2P",     "P2P-out"),
        Color =  c("#e41a1c",   "#377eb8", "#4daf4a",   "#984ea3",       "#ff7f00", "#ffff33", "#a65628", "#f781bf"));

# For paper https://hal.inria.fr/hal-01474556/document
#        Kernel = c("L2L",     "L2P",     "M2L_in",  "M2L_out", "M2M",     "P2M",     "P2P_in",  "P2P_out"),
#        Color =  c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00", "#ffff33", "#a65628", "#f781bf"));
}

library(RColorBrewer);
starpu_colors <- function()
{
  pre_colors <- brewer.pal(12, "Set3");
  pre_colors[13] = "#000000"
  pre_colors[14] = "#000000"
  tibble(Value = starpu_states()) %>%
      # Get colors from Set3
      mutate(Color = pre_colors) %>%
      # Adopt Luka suggestion: Idle = orange; Sleeping = rose
      mutate(Color = case_when(Value == "Idle" ~ "#FDB462",
                               Value == "PushingOutput" ~ "#BEBADA",
                               TRUE ~ Color)) -> t;
    # Transform to a nice named list for ggplot
    ret <- t %>% pull(Color)
    names(ret) <- t %>% pull(Value);
    return(ret);
}

cholesky_pastix_colors <- function()
{
    tibble(
        Kernel = c("blok_dpotrfsp1d_panel",
                   "cblk_dpotrfsp1d_panel",
                   "blok_dtrsmsp",
                   "blok_dgemmsp",
                   "cblk_dgemmsp"),
        Color = c("#e41a1c",
                  "#000000",
                  "#377eb8",
                  "#4daf4a",
                  "#c0c0c0"));
}

qrmumps_states_level_order <- function ()
{
    c(
        "Do_subtree",
        "INIT",
        "GEQRT",
        "GEMQRT",
        "TPQRT",
        "TPMQRT",
        "ASM",
        "CLEAN",
        "Idle");
}

qrmumps_states <- function ()
{
    c(
        "ASM",
        "GEMQRT",
        "Do_subtree",
        "CLEAN",
        "GEQRT",
        "INIT",
        "TPMQRT",
        "TPQRT",
        "Idle");
}

qrmumps_color_mapping <- function()
{
    #This vector changes the color ordering
    states = qrmumps_states();
    kcol <- data.frame(RGB=as.character(brewer.pal(9, "Set1")),
#These are only the color names I put manually here to try to understand the color mapping
#                       ColorName=c("red", "blue", "green", "purple", "orange", "yellow", "brown", "pink", "gray"),
                       StateName=factor(states, levels=qrmumps_states_level_order()),
                       xmin=1:length(states),
                       xmax=1:length(states)+1,
                       ymin=0,
                       ymax=1);
   kcol;
}

qrmumps_colors <- function()
{
qrmumps_color_mapping() %>%
    # Rename
    rename(Kernel = StateName, Color=RGB) %>%
    # Remove Idle
    filter(Kernel != "Idle") %>%
    # Change to character
    mutate(Kernel = as.character(Kernel), Color=as.character(Color)) %>%
    # Select only those necessary
    select(Kernel, Color) %>%
    # Change names according to latest modifications
    mutate(Kernel = case_when(
               .$Kernel == "ASM" ~ "assemble_block",
               .$Kernel == "GEMQRT" ~ "lapack_gemqrt",
               .$Kernel == "GEQRT" ~ "lapack_geqrt",
               .$Kernel == "TPMQRT" ~ "lapack_tpmqrt",
               .$Kernel == "TPQRT" ~ "lapack_tpqrt",
               .$Kernel == "Do_subtree" ~ "do_subtree",
               .$Kernel == "CLEAN" ~ "clean_front",
               .$Kernel == "INIT" ~ "init_front",
               TRUE ~ .$Kernel)) %>%
    # Add new kernels
    bind_rows (tibble(Kernel = c("init_block", "clean_block"),
                      Color = c("#FFFF33", "#984EA3")));
}
