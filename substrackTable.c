#include "stdint.h"
#include "tc-mydb.h"
#include "cdrDecoder.h"
#include "stdlib.h"
#include "unistd.h"
#include "string.h"
#include "tc-misc.h"
#include "time.h"
#include "tc-oam.h"
#include "tc-mydebug.h"

void *objFromSubstrack(struct _substrack *substrack)
{
  void *obj=NULL,*objA=NULL;
  uint8_t torLen=0, msisdnLen=0, cellIdLen=0, upOctetsLen=0, downOctetsLen=0, objLen=0;
  torLen = numBytesOcuped( substrack->tOR);
  msisdnLen = numBytesOcuped( substrack->msisdn);
  cellIdLen = numBytesOcuped( substrack->cellId);
  upOctetsLen = numBytesOcuped( substrack->upOctets);
  downOctetsLen = numBytesOcuped( substrack->downOctets);
  objLen = 1+torLen+1+msisdnLen+sizeof(uint16_t)+1+cellIdLen+1+upOctetsLen+1+downOctetsLen;
  obj = calloc( 1, 1+objLen);
  if( obj)
  {
    objA = obj;
    /*Copy objLen*/
    memcpy(objA,&objLen,1);
    objA += 1;
    /*Copy tOR len*/
    memcpy(objA,&torLen,1);
    objA += 1;
    /*Copy tOR*/
    memcpy(objA,&substrack->tOR,torLen);
    objA += torLen;
    /*Copy msisdn len*/
    memcpy(objA,&msisdnLen,1);
    objA += 1;
    /*Copy msisdn*/
    memcpy(objA,&substrack->msisdn,msisdnLen);
    objA += msisdnLen;
    /*Copy apnId*/
    memcpy(objA,&substrack->apnId,sizeof(uint16_t));
    objA += sizeof(uint16_t);
    /*Copy cellId len*/
    memcpy(objA,&cellIdLen,1);
    objA += 1;
    /*Copy cellId*/
    memcpy(objA,&substrack->cellId,cellIdLen);
    objA += cellIdLen;
    /*Copy upOctets len*/
    memcpy(objA,&upOctetsLen,1);
    objA += 1;
    /*Copy upOctets*/
    memcpy(objA,&substrack->upOctets,upOctetsLen);
    objA += upOctetsLen;
    /*Copy downOctets len*/
    memcpy(objA,&downOctetsLen,1);
    objA += 1;
    /*Copy downOctets*/
    memcpy(objA,&substrack->downOctets,downOctetsLen);
    objA += downOctetsLen;
  }
  return obj;
}
int substrackFromObj(struct _substrack *substrack, void *obj)
{
  int retCode=0;
  uint8_t len=0;
  uint8_t objLen=0;
  //void *objAux=NULL;
  //objAux = obj;
  /*Copy obj len*/
  memcpy(&objLen,obj,1);
  obj += 1;
  /*Copy tOR len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy tOR*/
  memcpy(&substrack->tOR,obj,len);
  obj += len;
  /*Copy msisdn len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy msisdn*/
  memcpy(&substrack->msisdn,obj,len);
  obj += len;
  /*Copy APN id*/
  memcpy(&substrack->apnId,obj,sizeof( uint16_t));
  obj += sizeof( uint16_t);
  /*Copy cellId len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy cellId*/
  memcpy(&substrack->cellId,obj,len);
  obj += len;
  /*Copy upOctets len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy upOctets*/
  memcpy(&substrack->upOctets,obj,len);
  obj += len;
  /*Copy downOctets len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy downOctets*/
  memcpy(&substrack->downOctets,obj,len);
  obj += len;
  ///*More fields*/
  //if( (objAux + len +1) > obj)
  //{
  //}
  return retCode;
}

int cmp_substrackIndex( void *pA, void *pB)
{
  int retCode=0;
  struct _substrack sA={0};
  struct _substrack sB={0};
  substrackFromObj(&sA,pA);
  substrackFromObj(&sB,pB);
  if( sA.msisdn > sB.msisdn)
    retCode = 1;
  else if( sA.msisdn < sB.msisdn)
    retCode = -1;
  else
  {
    if( sA.tOR > sB.tOR)
      retCode = 1;
    else if( sA.tOR < sB.tOR)
      retCode = -1;
  }
  return retCode;
}
time_t substrackLen=31*24*60*60;
int addSubstrack( struct _substrack *substrack)
{
  int retCode=0;
  time_t lTime=0;
  struct tm lTm={0};
  char tableName[64];
  struct _myDbFile_createTableAttr args={0};
  struct _myDbFile_res *res=NULL;
  struct _myDbFile_tuple tuple={0};
  time( &lTime);
  if( substrack->tOR >= (lTime-substrackLen))
  {
    tuple.obj = objFromSubstrack(substrack);
    if( tuple.obj)
    {
      strftime(tableName,sizeof(tableName),"substrack_%Y%m%d",localtime_r(&substrack->tOR,&lTm));
      do
      {
	res = myDbFile_insert( tableName, &tuple);
	if(res)
	{
	  if( (res->status == MYDBFILE_RES_FATALERROR) && (res->errorCode == MYDBFILE_ERROR_TABLENOTFOUND))
	  {
	    //printf( "MYDBFILE_ERROR_TABLENOTFOUND %s\n",tableName);
	    args.fixedRowLen = 0;
	    args.inFile = 1;
	    //args.inMemory = 1;
	    args.rowLenOctets = sizeof(uint8_t);
	    args.dbId = cdrDb_id;
	    strcpy(args.name,tableName);
	    strcpy(args.dir,cdrDbDir);
	    retCode = myDbFile_createTable( &args);
	    if( retCode >= 0)
	      retCode = MYDBFILE_ERROR_TABLENOTFOUND;
	  }
	  else
	    retCode = res->errorCode;
	  myDbFile_resClear( res);
	}
	else
	  retCode = MYDBFILE_ERROR_ENOMEM;
      }
      while(retCode == MYDBFILE_ERROR_TABLENOTFOUND);
      free( tuple.obj);
    }
    else
      retCode = MYDBFILE_ERROR_ENOMEM;
  }
  return retCode;
}
struct _substrack *getSubstrack( uint32_t msisdnId, time_t from, time_t to);
/*
	FREE struct
*/
int freeSubstrack( struct _substrack *substrack)
{
  int retCode=0;
  struct _substrack *substrackA=NULL;
  while(substrack)
  {
    substrackA = substrack->next;
    free( substrack);
    substrack = substrackA;
  }
  return retCode;
}
int cmp_substrackCust( void *pA, void *pB)
{
  int retCode=1;
  struct _substrack substrackA={0};
  struct _substrack substrackB={0};
  substrackFromObj(&substrackA,pA); 
  substrackFromObj(&substrackB,pB); 
  if( (substrackA.msisdn == substrackB.msisdn) && (substrackA.tOR >= substrackB.tOR))
    retCode=0;
  //  if( !retCode)
  //printf(" A.msisdnId %u, B.msisdnId %u\n",substrackA.msisdnId,substrackB.msisdnId);
  return retCode;
}
int cmp_substrackCust2( void *pA, void *pB)
{
  int retCode=1;
  struct _substrack substrackA={0};
  struct _substrack substrackB={0};
  substrackFromObj(&substrackA,pA); 
  substrackFromObj(&substrackB,pB); 
  if( substrackA.tOR > substrackB.tOR)
    retCode=-1;
  //  if( !retCode)
  //printf(" A.msisdnId %u, B.msisdnId %u\n",substrackA.msisdnId,substrackB.msisdnId);
  return retCode;
}
int getSubsTrack(struct _oamCommand *oamCmd, uint64_t msisdn, struct tm *fromTM, struct tm *toTM)
{
  int retCode=0;
  char tableName[128];
  char msgStr[256];
  char timeStr[32];
  char logStr[1024];
  time_t t1, t2;
  struct tm tM;
  struct _myDbFile_tuple tuple={0}, *tupleP=NULL;
  struct _myDbFile_res *res=NULL;
  struct _substrack substrack={0};
  //uint64_t msisdn=0;
  char apn[128];
  uint32_t apnIdAux=0;
  struct _cellTable cellTable={0};
  uint32_t cellIdAux=0;
  void *obj, *objAux;
  //unsigned int resDb=0;
  struct _myDB_obj *indexDB=NULL;
  struct _myDB_iteratorObj *it=NULL;
  t1 = mktime(fromTM);
  t2 = mktime(toTM);
    snprintf(logStr,sizeof(logStr),"FROM:%s %ld",asctime(fromTM),t1);
    debugLogging(DEBUG_CRITICAL,"oam",logStr);
    snprintf(logStr,sizeof(logStr),"TO:%s %ld",asctime(toTM),t2);
    debugLogging(DEBUG_CRITICAL,"oam",logStr);
  //printf(" t1 %ld\n",t1);
  //msisdn = querySubsMsisdn(CDRBUILTINDB_DEFAULT_MSISDNTABLE,msisdnId,&msisdn_mut);
  snprintf(msgStr,sizeof(msgStr),"timeOfReport,apn,rattype,mcc,mnc,lac/tac,ci/eci,upOctets,downOctets\r\n");
  retCode = oamSendMsg( msgStr,strlen(msgStr), oamCmd->procNo);
  if( retCode)
    return retCode;
  indexDB = myDB_newObj( sizeof(struct _substrack), 100000000, cmp_substrackCust2);
  while( (t2+(24*60*60)) >= t1)
  {
    localtime_r(&t1,&tM);
    strftime(tableName,sizeof(tableName),"substrack_%Y%m%d",&tM);
    //printf("%s\n",tableName);
    substrack.msisdn = msisdn;
    substrack.tOR = t2;
    obj = objFromSubstrack( &substrack);
    if( obj)
    {
      tuple.obj = obj;
      res = myDbFile_queryCust( tableName,&tuple,cmp_substrackCust);
      if( res)
      {
	if( res->status==MYDBFILE_RES_TUPLESOK)
	{
	  for(tupleP=res->objs;tupleP;tupleP=tupleP->next)
	  {
	    if( tupleP->obj)
	    {
	      substrack = (struct _substrack){0};
	      if(! substrackFromObj(&substrack,tupleP->obj))
	      {
	        if( substrack.tOR >= t1)
		{
		  objAux = objFromSubstrack( &substrack);
		  myDB_insertRowObj( indexDB, objAux);
		}
	      }
	    }
	  }
	}
	/*Clear res*/
	myDbFile_resClear( res);
      }
      else
	retCode = MYDBFILE_ERROR_ENOMEM;
      free( obj);
    }
    else
      retCode = MYDBFILE_ERROR_ENOMEM;
    t1 += 24*60*60;
  }
  /*Iterate*/
  it = myDB_createIteratorObj( indexDB);
  while( (obj=myDB_getNextRowObj( it)) && !retCode)
  {
    substrack = (struct _substrack){0};
    if(! substrackFromObj(&substrack,obj))
    {
      strftime(timeStr,sizeof(timeStr),"%Y-%m-%d %H:%M:%S",localtime_r(&substrack.tOR,&tM));
      if( apnIdAux != substrack.apnId)
      {
	apnIdAux = substrack.apnId;
	queryApn(CDRBUILTINDB_DEFAULT_APNTABLE,apn,sizeof(apn),substrack.apnId);
      }
      if( cellIdAux != substrack.cellId)
      {
	cellIdAux = substrack.cellId;
	cellTable.id = substrack.cellId;
	queryCell(CDRBUILTINDB_DEFAULT_CELLTABLE,&cellTable);
      }
      snprintf(msgStr,sizeof(msgStr),"%s,%s,%u,%u,%u,%u,%u,%lu,%lu\r\n",timeStr,apn,cellTable.ratt,cellTable.mcc,cellTable.mnc,cellTable.lac,cellTable.ci,substrack.upOctets,substrack.downOctets);
      retCode = oamSendMsg( msgStr,strlen(msgStr), oamCmd->procNo);
    }
  }
  myDB_destroyIteratorObj( it);
  myDB_dropObj( indexDB);
  //myDB_dropObj( resDb);

  if( !retCode)
  {
    snprintf(msgStr,sizeof(msgStr),",,,,,,,,\r\n");
    retCode = oamSendMsg( msgStr,strlen(msgStr), oamCmd->procNo);
  }
  return retCode;
}
