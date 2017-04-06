hell<-function(val){
  return(val);
}

InitEnv <- function() {

  closeAllConnections()
  rm(list = ls())
  library(magrittr)
  library(sparklyr)
  library(dplyr)
  library(ggplot2)
  library(caret)
  library(futile.logger)
  library(lubridate)
  library(C50)
  library(gbm)
  library(iterators)
  library(foreach)
  library(doParallel)
  library(ipred)
  library(pROC)
  library(ROSE)

  Sys.setenv(SPARK_HOME='/usr/hdp/2.4.2.0-258/spark')

  options(scipen = 999)
  options(digits = 3)

  config <- spark_config()
  config$spark.executor.cores <- 1
  config$spark.driver.cores <- 1
  config$spark.cores.max <- 2
  config$spark.executor.memory <- "3G"
  config$spark.executor.instances <- 3
  config$spark.scheduler.mode <-  "FAIR"
  config$spark.submit.deployMode <- "Cluster"

  spark_disconnect_all()
  sc <- spark_connect(master = "local", config = config, version = "1.6.2")

  assign('sc', sc, .GlobalEnv)
  flog.appender(appender.file("Output\\CBLog.log"), name = 'logger.b')
}

LoadDFForDemFinVars <- function() {

  sc <-get('sc', envir = .GlobalEnv)

  tbl_cache(sc, 'loan_datamart_transform_final')
  spark_RawDataFrame.tbl<-tbl(sc,'loan_datamart_transform_final')
  RawDataFrame <- spark_RawDataFrame.tbl

  RawDataFrame_sel <- select(RawDataFrame,
                             custid_finreference,
                             custdob,
                             age,
                             custtotalincome,
                             custgendercode,
                             custmaritalsts,
                             finisactive,
                             finstartdate,
                             maturitydate,
                             finreference,
                             fintype,
                             finbranch,
                             productcode,
                             assetdesc,
                             downpayment,
                             total_finamount,
                             total_financeprofit_amount,
                             balancedue_prinicipal,
                             balancedue_profit,
                             no_of_installments,
                             no_of_paid_inst,
                             no_of_od_inst,
                             paymentfrequency,
                             financestatus,
                             statusreason,
                             isdefaulter_r)


  a1 <- collect(RawDataFrame_sel)
  return(a1)
}

HandleNaNullNan <- function(RawDataFrame) {
  RawDataFrame <-a1
  n <- nrow(RawDataFrame)

  if (n < 1) {
    stop("The dataframe doesnt have any values lengths: ",
         length(RawDataFrame),
         ".")
  }

  lsCatnNumColNames <- GetCatnNumColNames(RawDataFrame)
  catVars <- lsCatnNumColNames$catVars
  numericVars_req <- lsCatnNumColNames$numericVars

  for (v in catVars) {
    pi <- paste('table Val of', v, sep = ' :')
    print(pi)
    print(table(RawDataFrame[, v]))
  }

  for (v in numericVars_req) {
    pi <- paste('table Val of', v, sep = ' :')
    print(pi)
    print(length(which(is.na(RawDataFrame[,v]))))
  }

  sapply(RawDataFrame[, vars], class)

  length(which(RawDataFrame$custempsts==""))
  # [1] 2
  length(which(RawDataFrame$override==""))
  # [1] 9
  length(which(RawDataFrame$execscore==""))
  # [1] 9
  length(which(RawDataFrame$custtotalexpense_bin_r==""))
  # [1] 376

  RawDataFrame$custempsts[RawDataFrame$custempsts==""] <- Mode(RawDataFrame$custempsts)
  RawDataFrame$override[RawDataFrame$override==""] <- Mode(RawDataFrame$override)
  RawDataFrame$execscore[RawDataFrame$execscore==""] <- Mode(RawDataFrame$execscore)
  RawDataFrame$custtotalexpense_bin_r[RawDataFrame$custtotalexpense_bin_r==""] <- "1-500"
  RawDataFrame$maxnumods_r[is.na(RawDataFrame$maxnumods_r)] <-     0

  return(RawDataFrame)
}

GetCatnNumColNames <- function(RawDataFrame) {
  # Pass a df and you get Cat and Num cols that are imp
  #
  # Args:
  #   RawDataFrame: DF which has to be explored
  #
  # Returns:
  #   Cat and Num cols list

  vars <- colnames(RawDataFrame)

  catVars <- vars[sapply(RawDataFrame[, vars], class) %in%
                    c('factor', 'character')]
  catVars_Notreq <-
    catVars[grepl("ODAmount|ODDays|Actual.Pay|Schd.Pay", catVars)]
  catVars_req <- setdiff(catVars, catVars_Notreq)
  catVars <- catVars_req

  numericVars <- vars[sapply(RawDataFrame[, vars], class) %in%
                        c('numeric', 'integer')]

  numericVars_Notreq <-
    numericVars[grepl("ODAmount|ODDays|Actual.Pay|Schd.Pay", numericVars)]
  numericVars_req <- setdiff(numericVars, numericVars_Notreq)

  catVars_ODDays <- vars[grepl("ODDays", vars)]
  catVars_ODAmount <- vars[grepl("ODAmount", vars)]
  catVars_Actual.Pay <- vars[grepl("Actual.Pay", vars)]
  catVars_Schd.Pay <- vars[grepl("Schd.Pay", vars)]
  vars1200 <-
    c(catVars_ODDays,
      catVars_ODAmount,
      catVars_Actual.Pay,
      catVars_Schd.Pay)
  non1200vars <- setdiff(vars, vars1200)

  FeatEngVars.a <- vars[endsWith(vars, ".a")]
  FeatEngVars.r <- vars[endsWith(vars, ".r")]
  rating1Vars <- c(
    "age",
    "CustTotalIncome",
    "CustGenderCode",
    "CustMaritalSts",
    "CustCtgCode",
    "CustEmpSts",
    "CustTypeCode",
    "custSector",
    "CustIndustry",
    "total_FinAmount",
    "No_Of_Installments",
    "Number_of_Finances_per_cust",
    "CreditWorth",
    "Override",
    "ExecScore",
    "custTypeDesc",
    "DSCR"
  )

  # these have no relevance and also these haev only one level ref
  # Error in `contrasts<-`(`*tmp*`, value = contr.funs[1 + isOF[nn]]) :
  #   contrasts can be applied only to factors with 2 or more levels
  rating2Vars <- c(
    "Custid_FinReference",
    "custdob",
    "FinReference",
    "FinStartDate",
    "MaturityDate",
    "MigratedFinance",
    "FinDivision",
    "CustCtgCode",
    "noOfDependents",
    "Currency",
    "fileName",
    "LoanStage"
  )

  borutavars <-
    c(
      "OD_Days_as_on_today"       ,
      "No_Of_OD_Inst"             ,
      "FinanceStatus"              ,
      "FinanceWorstStatus"        ,
      "custWorstSts"             ,
      "OD_Principal_amount.bin.r",
      "OD_Principal_amount.log1.r",
      "OD_amt_as_on_today.bin.r" ,
      "OD_amt_as_on_today.log1.r",
      "ODDays1"                  ,
      "ODDays2"                   ,
      "ODDays3"                  ,
      "ODDays4"                  ,
      "ODDays5"                  ,
      "ODDays6"                   ,
      "ODDays7"                  ,
      "ODDays8"                  ,
      "ODDays9"                  ,
      "Actual_Pay300"             ,
      "Schd_Pay19"               ,
      "Schd_Pay28"               ,
      "Schd_Pay31"               ,
      "Schd_Pay33"                ,
      "Schd_Pay34"               ,
      "Schd_Pay36"               ,
      "Schd_Pay82"               ,
      "MaxNumODs.r"               ,
      "NumODsInSeq.r"            ,
      "MaxODAmts.r"              ,
      "NumODAmtsInSeq.r"
    )
  rfeVars <-
    c("MaxNumODs.r",
      "ODDays1"  ,
      "FinanceWorstStatus" ,
      "ODDays2"    ,
      "ODDays4"   ,
      "ODDays3"
    )

  has.alt.r.values <- c(
    "Takaful_Insurance_Amount",
    "DSCR",
    "CustTotalExpense",
    "DownPayment",
    "total_FinAmount",
    "total_FinanceProfit_Amount",
    "BalanceDue_principal",
    "BalanceDue_profit",
    "OD_Principal_amount",
    "OD_Profit_amount",
    "OD_amt_as_on_today",
    "age",
    "CustTotalIncome"
  )

  dgLoanVars <- c(
    "age.bin.r"    ,
    "CustGenderCode",
    "CustMaritalSts",
    "CustEmpSts",
    "CustTypeCode",
    "custSector",
    "CustIndustry",
    "No_Of_Installments",
    "FinanceStatus" ,
    "FinanceWorstStatus"        ,
    "CreditWorth"                 ,
    "Override"                     ,
    "ExecScore"                     ,
    "custTypeDesc"                   ,
    "CustTotalIncome.bin.r"           ,
    "DownPayment.bin.r"               ,
    "total_FinAmount.bin.r"            ,
    "CustTotalExpense.bin.r"           ,
    "DSCR.bin.r"                       ,
    "NumODs.r"                          ,
    "MaxNumODs.r"                      ,
    "NumODsInSeq.r"                    ,
    "NumODAmts.r"                      ,
    "MaxODAmts.r"                      ,
    "NumODAmtsInSeq.r",
    "Schd_Pay1"
  )
  demFinVars <- c(
    "age.bin.r"    ,
    "CustGenderCode",
    "CustMaritalSts",
    "CustEmpSts",
    "CustTypeCode",
    "custSector",
    "CustIndustry",
    "No_Of_Installments",
    "CreditWorth"                 ,
    "Override"                     ,
    "ExecScore"                     ,
    "custTypeDesc"                   ,
    "CustTotalIncome.bin.r"           ,
    "DownPayment.bin.r"               ,
    "total_FinAmount.bin.r"            ,
    "CustTotalExpense.bin.r"           ,
    "FinanceStatus" ,
    "FinanceWorstStatus",
    "NumODs.r"                          ,
    "MaxNumODs.r"                      ,
    "NumODsInSeq.r"                    ,
    "DSCR.bin.r"                       ,
    "Schd_Pay1"
  )

  log1Conv.Amts <- vars[grepl("log1.r", vars)]
  binned.vars <- vars[grepl("bin.r", vars)]

  lsCatnNumColNames <- list()

  lsCatnNumColNames <-
    list(
      catVars = catVars,
      numericVars_req = numericVars_req,
      catVars_Notreq = catVars_Notreq,
      catVars_ODDays = catVars_ODDays,
      catVars_ODAmount = catVars_ODAmount,
      catVars_Actual.Pay = catVars_Actual.Pay,
      catVars_Schd.Pay = catVars_Schd.Pay,
      FeatEngVars.a = FeatEngVars.a,
      FeatEngVars.r = FeatEngVars.r,
      vars1200 = vars1200,
      non1200vars = non1200vars,
      rating1Vars = rating1Vars,
      rating2Vars = rating2Vars,
      borutavars = borutavars,
      rfeVars = rfeVars,
      has.alt.r.values = has.alt.r.values,
      log1Conv.Amts = log1Conv.Amts,
      binned.vars = binned.vars,
      dgLoanVars=dgLoanVars,
      demFinVars= demFinVars
    )



  return(lsCatnNumColNames)

}

BuildModeloDf <- function(RawDataFrame, nameOfDev){

  a1 <- RawDataFrame

  table(RawDataFrame$isdefaulter_r)
  table(a1$isdefaulter_r)

  nSlice <- 1
  seed <- 35

  cv.ctrl <- trainControl(
    method = "repeatedcv",
    repeats = 3,
    number = 10,
    summaryFunction = twoClassSummary,
    savePredictions = TRUE,
    classProbs = TRUE
  )
  set.seed(seed)
  registerDoParallel(4, cores = 4)
  getDoParWorkers()
  a1$closingstatus <- as.factor(a1$closingstatus)
  svm.tune <- train(
    isdefaulter_r ~ . ,
    data = a1,
    method = "svmRadial",
    tuneLength = 9,
    preProcess = c("center", "scale"),
    metric = "ROC",
    trControl = cv.ctrl
  )
  # svm.tune



  saveRDS(svm.tune,file = paste("PredictionModel.rds"))
}

PredictAIB <- function(val,ModelPath) {

  val <- if(is.character(val) && file.exists(val)){
    read.csv(val,colClasses=c("custsector"="character"))
  }
  else {
    as.data.frame(val)
  }



  glm.tune.1 <- readRDS(ModelPath)

  val$IsDefaulterPred.r <- predict(glm.tune.1, val)
  glm.probs <- predict(glm.tune.1, val, type = "prob")
  val$IsDefaulterPred.r.No<-glm.probs$No
  val$IsDefaulterPred.r.Yes<-glm.probs$Yes
  return(val)
}
