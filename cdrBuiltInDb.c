#include "stdio.h"
#include "time.h"
#include "tc-mydb.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include <unistd.h>
#include "cdrDecoder.h"
#include "pthread.h"

#define MSISDN_TABLE_CMPFUNC 1
#define APN_TABLE_CMPFUNC 2
#define CELL_TABLE_CMPFUNC 3
#define USERCOMB_TABLE_CMPFUNC 4
#define NOTIFY_TABLE_CMPFUNC 5
#define CDRBUILTINDB_DEFAULT_DIR "."
char cdrDbDir[MYDBFILE_MAXTABLEDIRLEN] = CDRBUILTINDB_DEFAULT_DIR;
int cdrDb_id=0;
/*MSISDN table variables*/
uint32_t msisdnId=0;
unsigned char lockMsisdnTable=0;
int msisdnCdrTableId=0;
/*USER combination table variables*/
int userCombId=0;
unsigned char lockUserCombTable=0;
int userCombTableId=0;
/*APN table variables*/
uint16_t apnId=0;
unsigned char lockApnTable=0;
int apnCdrTableId=0;
/*Cell table variables*/
uint32_t cellId=0;
unsigned char lockCellTable=0;
int cellCdrTableId=0;
//uint32_t imsiId=0;
//unsigned char lockImsiTable=0;
//int imsiCdrTableId=0;
//
//uint32_t imeiId=0;
//unsigned char lockImeiTable=0;
//int imeiCdrTableId=0;
pthread_mutex_t apnMutex;
pthread_mutex_t cellMutex;

int cdrBuiltInDb_init(char *msg, int msgLen)
{
  int retCode=0;
  struct _myDbFile_objects *dbObjsList=NULL;
  struct _myDbFile_createTableAttr args={0};
  dbObjsList = calloc(1,sizeof(struct _myDbFile_objects));
  dbObjsList->id = MSISDN_TABLE_CMPFUNC;
  dbObjsList->obj = cmp_subsIdTable;
  retCode = myDbFile_addObjectToList(dbObjsList);
  if( retCode)
    return retCode;
  dbObjsList = calloc(1,sizeof(struct _myDbFile_objects));
  dbObjsList->id = APN_TABLE_CMPFUNC;
  dbObjsList->obj = cmp_apnIdTable;
  retCode = myDbFile_addObjectToList(dbObjsList);
  if( retCode)
    return retCode;
  dbObjsList = calloc(1,sizeof(struct _myDbFile_objects));
  dbObjsList->id = CELL_TABLE_CMPFUNC;
  dbObjsList->obj = cmp_cellIdTable;
  retCode = myDbFile_addObjectToList(dbObjsList);
  if( retCode)
    return retCode;
  dbObjsList = calloc(1,sizeof(struct _myDbFile_objects));
  dbObjsList->id = USERCOMB_TABLE_CMPFUNC;
  dbObjsList->obj = cmp_userCombTable;
  retCode = myDbFile_addObjectToList(dbObjsList);
  if( retCode)
    return retCode;
  dbObjsList = calloc(1,sizeof(struct _myDbFile_objects));
  dbObjsList->id = NOTIFY_TABLE_CMPFUNC;
  dbObjsList->obj = cmp_notifyCombTable;
  retCode = myDbFile_addObjectToList(dbObjsList);
  if( retCode)
    return retCode;
  /*Init DB*/
  retCode = myDbFile_init(cdrDbDir);
  if( retCode)
    return retCode;
  /*Create DB*/
  retCode = myDbFile_createDb(CDRBUILTINDB_DEFAULT_DBNAME);
  if( retCode == MYDBFILE_ERROR_DBALREADYEXIST)
  {
    
    retCode = myDbFile_getDbIdByName(CDRBUILTINDB_DEFAULT_DBNAME);
    //printf(" myDbFile_getDbIdByName %d\n",retCode);
    if( retCode > 0)
      cdrDb_id = retCode;
  }
  if( retCode<0)
    return retCode;
  cdrDb_id = retCode;
  //printf( "cdrDb_id %d\n",cdrDb_id);
  /*Create MSISDN table*/
  retCode = getMaxSubsId( CDRBUILTINDB_DEFAULT_MSISDNTABLE, &msisdnId);
  if( retCode == MYDBFILE_ERROR_TABLENOTFOUND)
  {
    bzero(&args,sizeof(struct _myDbFile_createTableAttr));
    args.fixedRowLen = 0;
    args.inFile = 1;
    args.inMemory = 1;
    args.rowLenOctets = sizeof(uint8_t);
    args.dbId = cdrDb_id;
    args.contentIndexStep = 1000;
    args.cmpFuncId = MSISDN_TABLE_CMPFUNC;
    strcpy(args.name,CDRBUILTINDB_DEFAULT_MSISDNTABLE);
    strcpy(args.dir,cdrDbDir);
    retCode = myDbFile_createTable( &args);
  }
  if( retCode < 0)
    return retCode;
  pthread_mutex_init(&msisdn_mut,NULL);
  /*Create APN table*/
  retCode = getMaxApnId( CDRBUILTINDB_DEFAULT_APNTABLE, &apnId);
  if( retCode == MYDBFILE_ERROR_TABLENOTFOUND)
  {
    bzero(&args,sizeof(struct _myDbFile_createTableAttr));
    args.fixedRowLen = 0;
    args.inFile = 1;
    args.inMemory = 1;
    args.rowLenOctets = sizeof(uint8_t);
    args.dbId = cdrDb_id;
    args.contentIndexStep = 1000;
    args.cmpFuncId = APN_TABLE_CMPFUNC;
    strcpy(args.name,CDRBUILTINDB_DEFAULT_APNTABLE);
    strcpy(args.dir,cdrDbDir);
    retCode = myDbFile_createTable( &args);
  }
  if( retCode < 0)
    return retCode;
  pthread_mutex_init(&apnMutex,NULL);
  /*Create CELL table*/
  retCode = getMaxCellId( CDRBUILTINDB_DEFAULT_CELLTABLE, &cellId);
  if( retCode == MYDBFILE_ERROR_TABLENOTFOUND)
  {
    bzero(&args,sizeof(struct _myDbFile_createTableAttr));
    args.fixedRowLen = 1;
    args.fixedRowOctets = 16;
    args.inFile = 1;
    args.inMemory = 1;
    //args.rowLenOctets = sizeof(uint8_t);
    args.dbId = cdrDb_id;
    args.contentIndexStep = 100;
    args.cmpFuncId = CELL_TABLE_CMPFUNC;
    strcpy(args.name,CDRBUILTINDB_DEFAULT_CELLTABLE);
    strcpy(args.dir,cdrDbDir);
    retCode = myDbFile_createTable( &args);
  }
  if( retCode < 0)
    return retCode;
  pthread_mutex_init(&cellMutex,NULL);
  /*Create user combination table*/
  bzero(&args,sizeof(struct _myDbFile_createTableAttr));
  args.fixedRowLen = 0;
  args.fixedRowOctets = 0;
  args.inFile = 1;
  args.inMemory = 1;
  args.rowLenOctets = sizeof(uint8_t);
  args.dbId = cdrDb_id;
  args.contentIndexStep = 1000;
  args.cmpFuncId = USERCOMB_TABLE_CMPFUNC;
  strcpy(args.name,CDRBUILTINDB_DEFAULT_USERCOMBTABLE);
  strcpy(args.dir,cdrDbDir);
  retCode = myDbFile_createTable( &args);
  if( retCode ==MYDBFILE_ERROR_TABLEALREADYEXIST)
    retCode = 0;
  if ( retCode > 0)
    retCode = 0;
  /*Create notification table*/
  bzero(&args,sizeof(struct _myDbFile_createTableAttr));
  args.fixedRowLen = 0;
  args.fixedRowOctets = 0;
  args.inFile = 1;
  args.inMemory = 1;
  args.rowLenOctets = sizeof(uint8_t);
  args.dbId = cdrDb_id;
  args.contentIndexStep = 10;
  args.cmpFuncId = NOTIFY_TABLE_CMPFUNC;
  strcpy(args.name,CDRBUILTINDB_DEFAULT_NOTIFYTABLE);
  strcpy(args.dir,cdrDbDir);
  retCode = myDbFile_createTable( &args);
  if( retCode ==MYDBFILE_ERROR_TABLEALREADYEXIST)
    retCode = 0;
  if ( retCode > 0)
    retCode = 0;
  return retCode;
}
