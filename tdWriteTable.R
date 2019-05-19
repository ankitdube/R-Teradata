###Write to Teradata
tdWriteTable <- function(tdConnection, tableName, writeDF, primaryIndex = colnames(writeDF[1]),
                         append = FALSE, decimalDigits = 2, batchSize = 100000)
{
  #tdConnection - An RJDBC connection with Teradata.
  #tableName - A string complete name of table to create in Teradata.
  #writeDF - A data frame or coercible to data frame table which is to be uploaded to Teradata.
  #primaryIndex - A character vector or string separated by commas which provides the primary indices for the created table. Default first column of writeDF.
  #append - Logical indicating if data should be appended to table. Default FALSE.
  #decimalDigits - Integer indicating number of digits following the decimal point for numerics.
  #batchSize - Integer indicating size of batches for the INSERT statement.
  
  ##Check correct data types.
  writeDF <- as.data.frame(writeDF, stringsAsFactors = FALSE)
  if(!is.data.frame(writeDF))
  {
    stop("writeDF must be data frame or coercible to data frame.")
  }
  
  tableName <- as.character(tableName)
  if(!is.character(tableName))
  {
    stop("tableName must be character or coercible to character.")
  }
  if(length(tableName) != 1)
  {
    stop("tableName must have length 1.")
  }
  
  decimalDigits <- as.integer(decimalDigits)
  if(!is.integer(decimalDigits))
  {
    stop("decimalDigits must be integer or coercible to integer")
  }
  if(length(decimalDigits) != 1)
  {
    stop("decimalDigits must have length 1.")
  }
  
  batchSize <- as.integer(batchSize)
  if(!is.integer(batchSize))
  {
    stop("batchSize must be integer or coercible to integer")
  }
  if(length(batchSize) != 1)
  {
    stop("batchSize must have length 1.")
  }
  
  ##Ensure rJava is uploaded
  listOfPackages <- c("rJava")
  newPackages <- listOfPackages[!(listOfPackages %in% rownames(installed.packages()))]
  if(length(newPackages)) {install.packages(newPackages)}
  lapply(listOfPackages, library, character.only = TRUE)
  
  ##Ensure no scientific notation
  options(scipen = 999)
  
  ##Handle Teradata reserved words
  tdReserved <- c(
    'ABORT',	'ABORTSESSION',	'ABS',	'ABSOLUTE',	'ACCESS_LOCK',	'ACCOUNT',	'ACOS',	'ACOSH',	'ACTION',	'ADD',	'ADD_MONTHS',	'ADMIN',	'AFTER',	'AGGREGATE',	'ALIAS',	'ALL',	'ALLOCATE',	'ALTER',	'AMP',	'AND',	'ANSIDATE',	'ANY',	'ARE',	'ARRAY',	'AS',	'ASC',	'ASIN',	'ASINH',	'ASSERTION',	'AT',	'ATAN',	'ATAN2',	'ATANH',	'ATOMIC',	'AUTHORIZATION',	'AVE',	'AVERAGE',	'AVG',																								
    'BEFORE',	'BEGIN',	'BETWEEN',	'BINARY',	'BIT',	'BLOB',	'BOOLEAN',	'BOTH',	'BREADTH',	'BT',	'BUT',	'BY',	'BYTE',	'BYTEINT',	'BYTES',																																															
    'CALL',	'CASCADE',	'CASCADED',	'CASE',	'CASE_N',	'CASESPECIFIC',	'CAST',	'CATALOG',	'CD',	'CHAR',	'CHAR_LENGTH',	'CHAR2HEXINT',	'CHARACTER',	'CHARACTER_LENGTH',	'CHARACTERS',	'CHARS',	'CHECK',	'CHECKPOINT',	'CLASS',	'CLOB',	'CLOSE',	'CLUSTER',	'CM',	'COALESCE',	'COLLATE',	'COLLATION',	'COLLECT',	'COLUMN',	'COMMENT',	'COMMIT',	'COMPLETION',	'COMPRESS',	'CONNECT',	'CONNECTION',	'CONSTRAINT',	'CONSTRAINTS',	'CONSTRUCTOR',	'CONTINUE',	'CONVERT_TABLE_HEADER',	'CORR',	'CORRESPONDING',	'COS',	'COSH',	'COUNT',	'COVAR_POP',	'COVAR_SAMP',	'CREATE',	'CROSS',	'CS',	'CSUM',	'CT',	'CUBE',	'CURRENT',	'CURRENT_DATE',	'CURRENT_PATH',	'CURRENT_ROLE',	'CURRENT_TIME',	'CURRENT_TIMESTAMP',	'CURRENT_USER',	'CURSOR',	'CV',	'CYCLE',
    'DATA',	'DATABASE',	'DATABLOCKSIZE',	'DATE',	'DATEFORM',	'DAY',	'DEALLOCATE',	'DEC',	'DECIMAL',	'DECLARE',	'DEFAULT',	'DEFERRABLE',	'DEFERRED',	'DEGREES',	'DEL',	'DELETE',	'DEPTH',	'DEREF',	'DESC',	'DESCRIBE',	'DESCRIPTOR',	'DESTROY',	'DESTRUCTOR',	'DETERMINISTIC',	'DIAGNOSTIC',	'DIAGNOSTICS',	'DICTIONARY',	'DISABLED',	'DISCONNECT',	'DISTINCT',	'DO',	'DOMAIN',	'DOUBLE',	'DROP',	'DUAL',	'DUMP',	'DYNAMIC',																									
    'EACH',	'ECHO',	'ELSE',	'ELSEIF',	'ENABLED',	'END',	'END-EXEC',	'EQ',	'EQUALS',	'ERROR',	'ERRORFILES',	'ERRORTABLES',	'ESCAPE',	'ET',	'EVERY',	'EXCEPT',	'EXCEPTION',	'EXEC',	'EXECUTE',	'EXISTS',	'EXIT',	'EXP',	'EXPLAIN',	'EXTERNAL',	'EXTRACT',																																					
    'FALLBACK',	'FALSE',	'FASTEXPORT',	'FETCH',	'FIRST',	'FLOAT',	'FOR',	'FOREIGN',	'FORMAT',	'FOUND',	'FREE',	'FREESPACE',	'FROM',	'FULL',	'FUNCTION',																																															
    'GE',	'GENERAL',	'GENERATED',	'GET',	'GIVE',	'GLOBAL',	'GO',	'GOTO',	'GRANT',	'GRAPHIC',	'GROUP',	'GROUPING',	'GT',																																																	
    'HANDLER',	'HASH',	'HASHAMP',	'HASHBAKAMP',	'HASHBUCKET',	'HASHROW',	'HAVING',	'HELP',	'HOST',	'HOUR',																																																				
    'IDENTITY',	'IF',	'IGNORE',	'IMMEDIATE',	'IN',	'INCONSISTENT',	'INDEX',	'INDICATOR',	'INITIALIZE',	'INITIALLY',	'INITIATE',	'INNER',	'INOUT',	'INPUT',	'INS',	'INSERT',	'INSTEAD',	'INT',	'INTEGER',	'INTEGERDATE',	'INTERSECT',	'INTERVAL',	'INTO',	'IS',	'ISOLATION',	'ITERATE',																																				
    'JOIN',	'JOURNAL',																																																												
    'KEY',	'KURTOSIS',																																																												
    'LANGUAGE',	'LARGE',	'LAST',	'LATERAL',	'LE',	'LEADING',	'LEAVE',	'LEFT',	'LESS',	'LEVEL',	'LIKE',	'LIMIT',	'LN',	'LOADING',	'LOCAL',	'LOCALTIME',	'LOCALTIMESTAMP',	'LOCATOR',	'LOCK',	'LOCKING',	'LOG',	'LOGGING',	'LOGON',	'LONG',	'LOOP',	'LOWER',	'LT',																																			
    'MACRO',	'MAP',	'MATCH',	'MAVG',	'MAX',	'MAXIMUM',	'MCHARACTERS',	'MDIFF',	'MERGE',	'MIN',	'MINDEX',	'MINIMUM',	'MINUS',	'MINUTE',	'MLINREG',	'MLOAD',	'MOD',	'MODE',	'MODIFIES',	'MODIFY',	'MODULE',	'MONITOR',	'MONRESOURCE',	'MONSESSION',	'MONTH',	'MSUBSTR',	'MSUM',	'MULTISET',																																		
    'NAMED',	'NAMES',	'NATIONAL',	'NATURAL',	'NCHAR',	'NCLOB',	'NE',	'NEW',	'NEW_TABLE',	'NEXT',	'NO',	'NONE',	'NOT',	'NOWAIT',	'NULL',	'NULLIF',	'NULLIFZERO',	'NUMERIC',																																												
    'OBJECT',	'OBJECTS',	'OCTET_LENGTH',	'OF',	'OFF',	'OLD',	'OLD_TABLE',	'ON',	'ONLY',	'OPEN',	'OPERATION',	'OPTION',	'OR',	'ORDER',	'ORDINALITY',	'OUT',	'OUTER',	'OUTPUT',	'OVER',	'OVERLAPS',	'OVERRIDE',																																									
    'PAD',	'PARAMETER',	'PARAMETERS',	'PARTIAL',	'PASSWORD',	'PATH',	'PERCENT',	'PERCENT_RANK',	'PERM',	'PERMANENT',	'POSITION',	'POSTFIX',	'PRECISION',	'PREFIX',	'PREORDER',	'PREPARE',	'PRESERVE',	'PRIMARY',	'PRIOR',	'PRIVATE',	'PRIVILEGES',	'PROCEDURE',	'PROFILE',	'PROPORTIONAL',	'PROTECTION',	'PUBLIC',																																				
    'QUALIFIED',	'QUALIFY',	'QUANTILE',																																																											
    'RADIANS',	'RANDOM',	'RANGE_N',	'RANK',	'READ',	'READS',	'REAL',	'RECURSIVE',	'REF',	'REFERENCES',	'REFERENCING',	'REGR_AVGX',	'REGR_AVGY',	'REGR_COUNT',	'REGR_INTERCEPT',	'REGR_R2',	'REGR_SLOPE',	'REGR_SXX',	'REGR_SXY',	'REGR_SYY',	'RELATIVE',	'RELEASE',	'RENAME',	'REPEAT',	'REPLACE',	'REPLICATION',	'REPOVERRIDE',	'REQUEST',	'RESTART',	'RESTORE',	'RESTRICT',	'RESULT',	'RESUME',	'RET',	'RETRIEVE',	'RETURN',	'RETURNS',	'REVALIDATE',	'REVOKE',	'RIGHT',	'RIGHTS',	'ROLE',	'ROLLBACK',	'ROLLFORWARD',	'ROLLUP',	'ROUTINE',	'ROW',	'ROW_NUMBER',	'ROWID',	'ROWS',												
    'SAMPLE',	'SAMPLEID',	'SAVEPOINT',	'SCHEMA',	'SCOPE',	'SCROLL',	'SEARCH',	'SECOND',	'SECTION',	'SEL',	'SELECT',	'SEQUENCE',	'SESSION',	'SESSION_USER',	'SET',	'SETRESRATE',	'SETS',	'SETSESSRATE',	'SHOW',	'SIN',	'SINH',	'SIZE',	'SKEW',	'SMALLINT',	'SOME',	'SOUNDEX',	'SPACE',	'SPECIFIC',	'SPECIFICTYPE',	'SPOOL',	'SQL',	'SQLEXCEPTION',	'SQLSTATE',	'SQLTEXT',	'SQLWARNING',	'SQRT',	'SS',	'START',	'STARTUP',	'STATE',	'STATEMENT',	'STATIC',	'STATISTICS',	'STDDEV_POP',	'STDDEV_SAMP',	'STEPINFO',	'STRING_CS',	'STRUCTURE',	'SUBSCRIBER',	'SUBSTR',	'SUBSTRING',	'SUM',	'SUMMARY',	'SUSPEND',	'SYSTEM_USER',							
    'TABLE',	'TAN',	'TANH',	'TBL_CS',	'TEMPORARY',	'TERMINATE',	'THAN',	'THEN',	'THRESHOLD',	'TIME',	'TIMESTAMP',	'TIMEZONE_HOUR',	'TIMEZONE_MINUTE',	'TITLE',	'TO',	'TRACE',	'TRAILING',	'TRANSACTION',	'TRANSLATE',	'TRANSLATE_CHK',	'TRANSLATION',	'TREAT',	'TRIGGER',	'TRIM',	'TRUE',	'TYPE',																																				
    'UC',	'UNDEFINED',	'UNDER',	'UNDO',	'UNION',	'UNIQUE',	'UNKNOWN',	'UNNEST',	'UNTIL',	'UPD',	'UPDATE',	'UPPER',	'UPPERCASE',	'USAGE',	'USER',	'USING',																																														
    'VALUE',	'VALUES',	'VAR_POP',	'VAR_SAMP',	'VARBYTE',	'VARCHAR',	'VARGRAPHIC',	'VARIABLE',	'VARYING',	'VIEW',	'VOLATILE',																																																			
    'WAIT',	'WHEN',	'WHENEVER',	'WHERE',	'WHILE',	'WIDTH_BUCKET',	'WITH',	'WITHOUT',	'WORK',	'WRITE',																																																				
    'YEAR',																																																													
    'ZEROIFNULL',	'ZONE'
  )
  
  ##Make column names Teradata compliant
  colNames <- colnames(writeDF)
  colNames <- toupper(colNames)
  colNames <- trimws(colNames)
  colNames <- gsub("[[:space:]]", "_", colNames)
  colNames <- gsub(".", "_", colNames, fixed = TRUE)
  colNames <- gsub("[(){}]", "_", colNames)
  colNames[colNames %in% tdReserved] <- paste0("\"", colNames[colNames %in% tdReserved], "\"")
  
  ##Make primary indices Teradata compliant
  if(!append)
  {
    primaryIndex <- as.character(primaryIndex)
    if(!is.character(primaryIndex))
    {
      stop("primaryIndex must be character or coercible to character.")
    }
    primaryIndex <- strsplit(primaryIndex, ",")
    tempIndex <- NULL
    for(i in 1:length(primaryIndex))
    {
      tempIndex <- c(tempIndex, primaryIndex[[i]])
    }
    primaryIndex <- tempIndex
    
    primaryIndex <- primaryIndex(writeDF)
    primaryIndex <- toupper(primaryIndex)
    primaryIndex <- trimws(primaryIndex)
    primaryIndex <- gsub("[[:space:]]", "_", primaryIndex)
    primaryIndex <- gsub(".", "_", primaryIndex, fixed = TRUE)
    primaryIndex <- gsub("[(){}]", "_", primaryIndex)
    primaryIndex[primaryIndex %in% tdReserved] <- paste0("\"", primaryIndex[primaryIndex %in% tdReserved], "\"")
    
    if(!all(primaryIndex %in% colNames))
    {
      stop("primaryIndex must be in column names of writeDF")
    }
  }
  
  ##Remove duplicate rows
  if(any(duplicated(writeDF)))
  {
    writeDF <- unique(writeDF)
    warning("Duplicated rows removed.")
  }
  
  ##Handle empty columns
  emptyCheck <- sapply(writeDF, function(x) {all(is.na(x))})
  writeDF[, emptyCheck] <- ""
  
  ##Get SQL structure
  strDF <- sapply(writeDF, class)
  if(is.list(strDF))
  {
    for(i in 1:length(strDF))
    {
      if(length(strDF[[i]]) > 1)
      {
        strDF[[i]] <- "POSIXct"
      }
    }
  }
  strDF <- as.character(strDF)
  strJava <- strDF
  
  ##Integer
  isInt <- strDF == "integer"
  strDF[isInt] <- "int"
  ##Factor
  isFactor <- strDF == "factor"
  if(sum(isFactor) > 1)
  {
    writeDF[, isFactor] <- sapply(writeDF[, isFactor], as.character)
  }else if(sum(isFactor))
  {
    writeDF[, isFactor] <- as.character(writeDF[, isFactor])
  }
  strDF[isFactor] <- "character"
  ##Character
  isCharacter <- strDF == "character"
  if(!nrow(writeDF))
  {
    strDF[isCharacter] <- "varchar (255)"
  }else
  {
    if(sum(isCharacter) > 1)
    {
      writeDF[, isCharacter] <- paste0("varchar (", sapply(writeDF[, isCharacter], 
                                                           function(x) {max(nchar(x), na.rm = TRUE)},
                                                           ")"))
    }else if(sum(isCharacter))
    {
      writeDF[, isCharacter] <- paste0("varchar (", max(nchar(writeDF[, isCharacter]), na.rm = TRUE), 
                                                           ")")
    }
  }
  ##Numeric
  isNumeric <- strDF == "numeric"
  if(!nrow(writeDF))
  {
    strDF[isNumeric] <- paste0("decimal (38, ", decimalDigits,")")
  }else
  {
    if(sum(isNumeric) > 1)
    {
      writeDF[, isNumeric] <- paste0("decimal (", sapply(writeDF[, isNumeric], 
                                                           function(x) {max(nchar(x), na.rm = TRUE)} + decimalDigits,
                                                           ", ", decimalDigits, ")"))
    }else if(sum(isNumeric))
    {
      writeDF[, isNumeric] <- paste0("decimal (", max(nchar(writeDF[, isNumeric]), na.rm = TRUE) + decimalDigits, 
                                       ", ", decimalDigits, ")")
    }
  }
  ##Date
  isDate <- strDF == "Date"
  strDF[isDate] <- "DATE"
  ##Timestamp
  isTimestamp <- strDF == "POSIXct" | strDF == "POSIXt"
  strDF[isTimestamp] <- "TIMESTAMP"
  ##Empty
  strDF[emptyCheck] <- "varchar (1)"
  
  ##Create empty table
  if(!append)
  {
    createState <- paste0("CREATE TABLE ", tableName, " (")
    createTemp <- paste(paste(colNames, strDF), collapse = "\n")
    primaryIndex <- paste(primaryIndex, collapse = ", ")
    createState <- paste0(createState, createTemp, "\n primary index (",
                          primaryIndex, ");")
    dbSendUpdate(tdConnection, createState)
    
    if(!nrow(writeDF))
    {
      return(paste("No data", tableName, "create as empty table"))
    }
  }
  
  ##Create insert statement
  .jcall(tdConnection@jc, "V", "setAutoCommit", FALSE)
  insertState <- paste0("INSERT INTO ", tableName, " values (",
                        paste(rep("?", length(colNames)), collapse = ", "), ")")
  ps <- .jcall(tdConnection@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", insertState)
  
  ##Get Java structures
  strJava[isInt] <- "setInt"
  strJava[isCharacter] <- "setString"
  strJava[isDate] <- "setDate"
  strJava[isNumeric] <- "setDouble"
  strJava[isTimestamp] <- "setTimestamp"
  
  ##Insert data
  for(i in 1:nrow(writeDF))
  {
    for(j in 1:length(colNames))
    {
      if(isDate[j])
      {
        tempDate <- .jnew("java/sql/Date", .jlong(as.numeric(as.POSIXct(writeDF[i, j])) * 1000))
        .jcall(ps, "V", strJava[j], as.integer(j), tempDate)
      }else if(isTimestamp[j])
      {
        tempTimestamp <- .jnew("java/sql/Timestamp", .jlong(as.numeric(as.POSIXct(writeDF[i, j])) * 1000))
        .jcall(ps, "V", strJava[j], as.integer(j), tempTimestamp)
      }else
      {
        .jcall(ps, "V", strJava[j], as.integer(j), writeDF[i, j])
      }
    }
    .jcall(ps, "V", "addBatch")
    if(i %% batchSize == 0)
    {
      .jcall(ps, "[I", "executeBatch", check = FALSE)
    }else if(i == nrow(writeDF))
    {
      .jcall(ps, "[I", "executeBatch", check = FALSE)
    }
  }
  .jcall(tdConnection@jc, "V", "commit")
  .jcall(ps, "V", "close")
  .jcall(tdConnection@jc, "V", "setAutoCommit", TRUE)
  
  ##Indicate successful completion
  print(paste("Table upload of", tableName, "complete."))
}
