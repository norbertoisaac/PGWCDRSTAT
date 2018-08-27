#define _GNU_SOURCE           /* See feature_test_macros(7) */
#include "tc-mySubscriber.h"
#include "dirent.h"
#include "stdio.h"
#include "string.h"
#include "errno.h"
#include "stdint.h"
#include "stdlib.h"
#include "tc-asn1.h"
#include "tc-oam.h"
#include "unistd.h"
//#include "3gppCdr.h"
#include "time.h"
//#include "mydb.h"
#include "cdrDecoder.h"
#include "tc-mydebug.h"
#include <regex.h>
#include "unistd.h"
#include "tc-misc.h"
#include <pthread.h>
#include <sched.h>
#include <signal.h>
/*Global DB definition*/
int subscriberDB=0;
int notificationDb=0;
int subscriber_cdr_db=0;
int apn_cdr_db=0;
int cell_cdr_db=0;
int gw_cdr_db=0;
unsigned int subscriberCount=0;
struct _pgCon *globalPgCon=NULL;
struct _pgCon *cdrCompactPgCon=NULL;
long globalCpuNum=0;
char notifySvrName[128] = "10.150.31.68";
int cdrDecoderInit()
{
  int retCode=0;
  return retCode;
}
/*CDR thread struct information*/
struct _cdrProcess {
  /*Flags*/
  unsigned busy:1;/*Process busy*/
  unsigned run:1;/*Process run*/
  unsigned notifyStart:1;
  unsigned trace:1;
  unsigned tracking:1;
  unsigned trackSql:1;
  unsigned usageStat:1;
  unsigned pdpStat:1;
  unsigned pgwStatPrepared:1;
  unsigned trackingPrepared:1;
  /*Process id*/
  unsigned int id;
  /*System thread id*/
  pthread_t tId;
  /*Cpu Num*/
  int cpuCount;
  /*CPU set*/
  cpu_set_t cpuSet;
  /*CDR file path processing*/
  char cdrFilePathProcessing[1024];
  /*CDR file path error*/
  char cdrFilePathError[1024];
  /*CDR file path error*/
  char cdrFilePathReverted[1024];
  /*Original CDR file path*/
  char cdrFilePath[1024];
  /*Original CDR file name*/
  char cdrFileName[1024];
  /*Subscriber DB id*/
  int subsDbId;
  /*PostgreSQL connection for GPRS stat*/
  struct _pgCon *pgCon;
  /*PostgreSQL connection for GPRS stat*/
  //struct _pgCon *pgConStat;
  /*PostgreSQL connection for tracking*/
  struct _pgCon *pgConTrk;
  /*Compact process counter*/
  //unsigned int *compactJuobCounter;
  struct _cdrCompactArg *cCA;
  /*Next struct*/
  struct _cdrProcess *next;
};
int cmpMCC(void *a, void *b)
{
  uint16_t n1 = *(uint16_t *)a;
  uint16_t n2 = *(uint16_t *)b;
  if( n1==n2) return 0;
  else if( n1>n2) return 1;
  else return -1;
}
int cmpPlmnId(void *a, void *b)
{
  struct _plmnId n1 = *(struct _plmnId *)a;
  struct _plmnId n2 = *(struct _plmnId *)b;
  int r = 0;
  if( n1.mnc>n2.mnc) r=1;
  else if( n1.mnc<n2.mnc) r=-1;
  else
  {
    if( n1.mcc>n2.mcc) r=1;
    else if( n1.mcc<n2.mcc) r=-1;
  }
  //else if( n1>n2) return 1;
  return r;
}
int apnCmp(void *a, void *b)
{
  int r = 0;
  struct _apnId *aA = a;
  struct _apnId *aB = b;
  r = strcmp(aA->apnNi,aB->apnNi);
  if( r==0)
  {
    r = cmpPlmnId(&aA->plmn,&aB->plmn);
  }
  return r;
}
//struct _nodeId {
//  struct _plmnId plmn;
//  char nodeName[64];
//};
int cmpLotvTableName(void *a, void *b)
{
  int r=0;
  r = strcmp(a, b);
  return r;
}
int cmpLosdTableName(void *a, void *b)
{
  int r=0;
  struct _losdTables *A=a;
  struct _losdTables *B=b;
  r = strcmp(A->name, B->name);
  return r;
}
#define cmpLosdTableName cmpLotvTableName
#define cmpPgwcdrEventTableName cmpLotvTableName

int cmpNodeId(void *a, void *b)
{
  int r=0;
  struct _nodeId *nodeA = a;
  struct _nodeId *nodeB = b;
  r = strcmp(nodeA->nodeName, nodeB->nodeName);
  if( r==0)
  {
    r = cmpPlmnId(&nodeA->plmn, &nodeB->plmn);
  }
  return r;
}
//struct _snaddress {
//  struct _plmnId plmn;
//  struct in6_addr snaddress;
//  uint16_t sntype;
//};
int cmpSnaddress(void *a, void *b)
{
  int r=0;
  struct _snaddress *sA = a;
  struct _snaddress *sB = b;
  r = memcmp(sA->snaddress.s6_addr,sB->snaddress.s6_addr,16);
  if (r==0)
  {
    if(sA->sntype > sB->sntype) r=1;
    else if(sA->sntype < sB->sntype) r=-1;
  }
  return r;
}
//struct _gwaddress {
//  struct _plmnId plmn;
//  char nodeId[64];
//  struct in6_addr gwaddress;
//};
int cmpGwaddress(void *a, void *b)
{
  int r=0;
  struct _gwaddress *gA = a;
  struct _gwaddress *gB = b;
  r = memcmp(gA->gwaddress.s6_addr,gB->gwaddress.s6_addr,16);
  return r;
}
int cmpSubstrkTableName(void *a, void *b)
{
  int r=0;
  struct _substrkTableName *A=a;
  struct _substrkTableName *B=b;
  r = strcmp(A->tableName,B->tableName);
  return r;
}
//struct _cdrCi {
//  uint8_t ratType;
//  struct _userLocationInformation uli;
//};
int cmpCdrCi (void *a, void *b)
{
  int r=0;
  struct _cdrCi *cA= a;
  struct _cdrCi *cB= b;
  r = cmpUli( cA->uli, cB->uli);
  if( r==0)
  {
    if( cA->ratType > cB->ratType) r = 1;
    else if( cA->ratType < cB->ratType) r = -1;
  }
  return r;
};
int cmpCdr_pgwcdrcmb(void *a, void *b)
{
  int r=0;
  struct _cdr_pgwcdrcmb *A = a;
  struct _cdr_pgwcdrcmb *B = b;
  if(A->cdrCi_id > B->cdrCi_id) r=1;
  else if(A->cdrCi_id < B->cdrCi_id) r=-1;
  else if(A->snaddress_id > B->snaddress_id) r=1;
  else if(A->snaddress_id < B->snaddress_id) r=-1;
  else if(A->apnId_id > B->apnId_id) r=1;
  else if(A->apnId_id < B->apnId_id) r=-1;
  else if(A->gwaddress_id > B->gwaddress_id) r=1;
  else if(A->gwaddress_id < B->gwaddress_id) r=-1;
  else if(A->cc > B->cc) r=1;
  else if(A->cc < B->cc) r=-1;
  else if(A->dynamicAddressFlag > B->dynamicAddressFlag) r=1;
  else if(A->dynamicAddressFlag < B->dynamicAddressFlag) r=-1;
  else if(A->iMSsignalingContext > B->iMSsignalingContext) r=1;
  else if(A->iMSsignalingContext < B->iMSsignalingContext) r=-1;
  else if(A->apnSelectionMode > B->apnSelectionMode) r=1;
  else if(A->apnSelectionMode < B->apnSelectionMode) r=-1;
  else if(A->chChSelectionMode > B->chChSelectionMode) r=1;
  else if(A->chChSelectionMode < B->chChSelectionMode) r=-1;
  return r;
}
int cmpSubstrkSql(void *a, void *b)
{
  int retCode = 1;
  struct _substrkSql *A=a;
  struct _substrkSql *B=b;
  if(A->tor.tm_year > B->tor.tm_year) retCode = 1;
  else if(A->tor.tm_year < B->tor.tm_year) retCode = -1;
  else if(A->tor.tm_mon > B->tor.tm_mon) retCode = 1;
  else if(A->tor.tm_mon < B->tor.tm_mon) retCode = -1;
  else if(A->tor.tm_mday > B->tor.tm_mday) retCode = 1;
  else if(A->tor.tm_mday < B->tor.tm_mday) retCode = -1;
  else if(A->tor.tm_hour > B->tor.tm_hour) retCode = 1;
  else if(A->tor.tm_hour < B->tor.tm_hour) retCode = -1;
  else if(A->tor.tm_min > B->tor.tm_min) retCode = 1;
  else if(A->tor.tm_min < B->tor.tm_min) retCode = -1;
  else if(A->tor.tm_sec > B->tor.tm_sec) retCode = 1;
  else if(A->tor.tm_sec < B->tor.tm_sec) retCode = -1;
  return retCode;
}
void pqnotifyreceiver(void *arg, const PGresult *res)
{
  return;
}
/*
	CDR thread process
*/
void *cdrProcess( void *p1)
{
  if( !p1) return NULL;
  struct _cdrProcess *p=p1;
  int retCode=0;
  FILE *cdrFile=NULL;
  uint8_t *cdrContent=NULL;
  struct _userComb userComb={0};
  struct _cdrPGWRecordStatParams statParams;
  unsigned int cdrFileLen=0;
  unsigned int localTagCount=0, totalTagCount=0, pgwRecordCount=0;
  struct _asn1BerTag localAsn1BerTag={0}, /**resultBerTag=NULL,*/ targetBerTag={0};
  struct _pgwRecord pgwRecord={0};
  char cdrFilePathProcesed[1024];
  //char cdrFilePathError[1024];
  /*Counters*/
  unsigned int totalCosc=0;
  unsigned int totalCocc=0;
  unsigned int subsTrackCount=0;
  //int localErrno=0;
  struct {
    unsigned error:1;
    unsigned revertFile:1;
    unsigned paused:1;
  } localFlags={0};
  /*Time accounting*/
  time_t t0,t1,t2;
  /*Log string*/
  char logStr[1024];
  /*Temporary tables names*/
  //char cdr_ciDbTmpTable[64];
  char coccttn[128];
  //char coscttn[64];
  //char pgwcdrcmb_TmpTable[64];
  char subsTrackPrepare[32], pdpPrepareStr[32]/*, pdpTableName[64]*/;
  //struct _notifyInfo notifyInfo;
  //char notifyStr[2048];
  //char bodyStr[2048];
  //uint16_t notifyLen=0, bodyLen=0;
  //char timeStr[128];
  /*In memory PGW CDR stat DB*/
  int mccDB = myDB_createObj( sizeof(uint16_t),10, cmpMCC);
  int plmnDB = myDB_createObj( sizeof(struct _plmnId),10, cmpPlmnId);
  int apnDB = myDB_createObj(sizeof(struct _apnId),10,apnCmp);
  int nodeDB = myDB_createObj(sizeof(struct _nodeId),10,cmpNodeId);
  int snAddressDB = myDB_createObj(sizeof(struct _snaddress),10,cmpSnaddress);
  int gwAddressDB = myDB_createObj(sizeof(struct _gwaddress),10,cmpGwaddress);
  int ciDB = myDB_createObj(sizeof(struct _cdrCi),10,cmpCdrCi);
  int pgwCDRCoccDB = myDB_createObj( sizeof(struct _pgwRecordCOCC), 100, cmpPgwStatCOCC);
  int pgwCDRCoscDB = myDB_createObj( sizeof(struct _pgwRecordCOSC), 400, cmpPgwStatCOSC);
  int pdpStatDB = myDB_createObj( sizeof(struct _pdpStat), 400, cmpPdpStat);
  int lotvTablesDB = myDB_createObj( lotvTableLen, 10, cmpLotvTableName);
  int losdTablesDB = myDB_createObj( sizeof(struct _losdTables), 10, cmpLosdTableName);
  int pgwcdrEventTablesDB = myDB_createObj( pgwcdrEventTableLen, 10, cmpPgwcdrEventTableName);
  int cdr_pgwcdrcmbDB = myDB_createObj(sizeof(struct _cdr_pgwcdrcmb), 100, cmpCdr_pgwcdrcmb);
  int substrkTableNameDB = myDB_createObj(sizeof(struct _substrkTableName), 100, cmpSubstrkTableName);
  int substrkDB = myDB_createObj(sizeof(struct _substrkSql), 600, cmpSubstrkSql);
  uint16_t *cdr_mcc=NULL;
  struct _plmnId *cdr_plmnId=NULL;
  struct _apnId *cdr_apn = NULL, *cdr_apn_B = NULL;
  struct _nodeId *cdr_nodeId = NULL;
  struct _snaddress *snAddress = NULL, *snAddress_B = NULL;
  struct _gwaddress *gwAddress = NULL, *gwAddress_B = NULL;
  struct _cdrCi *cdr_ci = NULL, *cdr_ci_B = NULL;
  struct _cdr_pgwcdrcmb *cdr_pgwcdrcmb = NULL, *cdr_pgwcdrcmb_B;
  uint16_t gwaddress_id=0;
  uint16_t snaddress_id=0;
  uint32_t cdrCi_id=0;
  uint16_t apnId_id=0;
  uint64_t cdr_pgwcdrcmb_id=0;
  //uint32_t substrkSeq=0;
  uint32_t statSeq=0;
  char *substrk_buffer=NULL;
  char sql[20000];
  //uint32_t substrk_buffer_len=0, substrk_buffer_count=0;
  /*Iterators*/
  void *substrkTableNameIt = myDB_createIterator( substrkTableNameDB);
  void *coccDbIt = myDB_createIterator( pgwCDRCoccDB);
  void *coscDbIt = myDB_createIterator( pgwCDRCoscDB);
  void *pdpStatDbIt = myDB_createIterator( pdpStatDB);
  //void *it_mccDB = myDB_createIterator( mccDB);
  //void *it_plmnDB = myDB_createIterator( plmnDB);
  //void *it_apnDB = myDB_createIterator( apnDB);
  void *it_nodeDB = myDB_createIterator( nodeDB);
  //void *it_snAddressDB = myDB_createIterator( snAddressDB);
  //void *it_gwAddressDB = myDB_createIterator(gwAddressDB);
  //void *it_ciDB = myDB_createIterator(ciDB);
  void *it_lotvTableNameDB = myDB_createIterator(lotvTablesDB);
  void *it_losdTableNameDB = myDB_createIterator(losdTablesDB);
  //void *it_pgwcdrEventTablesDB = myDB_createIterator(pgwcdrEventTablesDB);
  //void *it_cdr_pgwcdrcmbDB = myDB_createIterator(cdr_pgwcdrcmbDB);
  void *it_substrkDB = myDB_createIterator(substrkDB);
  char ttn[64];
  //double substrkTDiff=0;
  struct tm substrkMinTor, substrkMaxTor;
  //time_t substrT;
  snprintf(logStr,sizeof(logStr),"pn=%d,Loading CDR Decoder Service threadId=0x%lX", p->id, p->tId);
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  /*Notify receiver*/
  ///*Prepared for acccounting*/
  //snprintf( acctStart, sizeof(acctStart), "acctStart%lx", p->tId);
  //acctStartPrepare(p->pgCon, acctStart);
  //snprintf( acctUpdate, sizeof(acctUpdate), "acctUpdate%lx", p->tId);
  //acctUpdatePrepare(p->pgCon,acctUpdate);
  //snprintf( acctStop, sizeof(acctStop), "acctStop%lx", p->tId);
  //acctStopPrepare(p->pgCon,acctStop);
  /*Infinite loop*/
  while( p->run)
  {
      if(!cdrDecParam.decoderOn)
      {
	snprintf(logStr,sizeof(logStr),"pn=%d,decoder halt", p->id);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	localFlags.paused=1;
      }
      while(!cdrDecParam.decoderOn) sleep(1);
      if(localFlags.paused)
      {
	snprintf(logStr,sizeof(logStr),"pn=%d,decoder run", p->id);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
        localFlags.paused=0;
      }
    /*Waiting for CDR file asigned to this process*/
    pause();
    //sleep(60);
    if( p->busy)
    {
      /*Clean registers*/
      bzero(&localFlags, sizeof(localFlags));
      retCode=0;
      totalCosc=0;
      totalCocc=0;
      gwaddress_id=0;
      snaddress_id=0;
      cdrCi_id=0;
      apnId_id=0;
      cdr_pgwcdrcmb_id=0;
      //substrkTDiff = 0;
      bzero(&substrkMinTor,sizeof(struct tm));
      bzero(&substrkMaxTor,sizeof(struct tm));
      if(substrk_buffer)
      {
        free(substrk_buffer);
	substrk_buffer=NULL;
      }
      //substrk_buffer_len=0;
      //substrk_buffer_count=0;
      /*SQL health check for PGW stat*/
      if( cdrDecParam.pwgStat)
      {
        /*Get SQL resource*/
	while( !p->pgCon)
	{
	  p->pgCon = myDB_selectPgconByGroup(cdrDecParam.statSqlGroup);
	  if(!p->pgCon )
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,no SQL resource available for stat", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    sleep(10);
	  }
	  else
	  {
	    p->pgCon->procId = p->id;
	    snprintf(logStr,sizeof(logStr),"pn=%d,SQL resource reserved for stat", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  }
	}
	/*Check SQL connection*/
	do
	{
	  if( myDB_pgLinkReset( p->pgCon))
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,fail to SQL link pgCon reset", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    sleep(10);
	  }
	  p->pgwStatPrepared = 0;
	}
	while( myDB_pgLinkStatus( p->pgCon));
	/*Prepared statements*/
	if( !p->pgwStatPrepared)
	{
	  time(&t0);
	  /*Creating temporary tables*/
	  /*LOTV temporary table*/
	  snprintf( coccttn, sizeof(coccttn), "cocctt_%lx_%ld", p->tId,t0);
	  retCode = cdrPGWCoccCreateTmpTable( p->pgCon, coccttn);
	  if( !retCode)
	    PQsetNoticeReceiver(p->pgCon->pgCon, pqnotifyreceiver,coccttn);
	  /*LOSD temporary table*/
	  //snprintf( coscttn, sizeof(coscttn), "cosctt%lx", p->tId);
	  //if( !retCode)
	  //  retCode = cdrPGWCoscCreateTmpTable( p->pgCon, coccttn);
	  /*Cell id temporary table*/
	  //snprintf( cdr_ciDbTmpTable, sizeof(cdr_ciDbTmpTable), "tmp_cdr_ci_%lx", p->tId);
	  if( !retCode)
	    retCode = cdrCiCreateTmpTable( p->pgCon, coccttn);
	  /*PGWCDR combination temporary table*/
	  //snprintf( pgwcdrcmb_TmpTable, sizeof(pgwcdrcmb_TmpTable), "pgwcdrcmb_TmpTable_%lx", p->tId);
	  if( !retCode)
	    retCode = pgwcdrcmb_CreateTmpTable( p->pgCon, coccttn);
	  /*GW address temporary table*/
	  if( !retCode)
	    retCode = gwAddress_tmpTable( p->pgCon, coccttn);
	  /*Serving node address temporaty table*/
	  if( !retCode)
	    retCode = snAddress_tmpTable( p->pgCon, coccttn);
	  /*APN temporaty table*/
	  if( !retCode)
	    retCode = apn_tmpTable( p->pgCon, coccttn);
	  /*Prepared SQL sentences*/
	  snprintf( pdpPrepareStr, sizeof(pdpPrepareStr), "pdpPrepared%08X", (unsigned int)p->tId);
	  if( !retCode)
	    retCode = pdpPrepare( p->pgCon,pdpPrepareStr);
	  if( !retCode)
	    retCode = cdrPGWSQLPrepareCocc( p->pgCon, coccttn);
	  //if( !retCode)
	  //  retCode = cdrPGWSQLPrepareCosc( p->pgCon, coccttn);
	  if( !retCode)
	    retCode = pgwcdrcmb_prepared( p->pgCon, coccttn);
	  if( !retCode)
	    p->pgwStatPrepared = 1;
	  else
	    localFlags.revertFile = 1;
	}
      }
      /*SQL health check for subscriber tracking*/
      if( cdrDecParam.subsTrack && cdrDecParam.subsTrackSql && !retCode)
      {
        /*Get SQL resource*/
        while( !p->pgConTrk)
	{
	  p->pgConTrk = myDB_selectPgconByGroup(cdrDecParam.strkSqlGroup);
	  if(!p->pgConTrk )
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,no SQL resource available for subscriber tracking", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    sleep(10);
	  }
	  else
	  {
	    p->pgConTrk->procId = p->id;
	    snprintf(logStr,sizeof(logStr),"pn=%d,SQL resource reserved for tracking", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  }
	}
	/*Check SQL connection*/
	do
	{
	  p->trackingPrepared = 0;
	  if( myDB_pgLinkReset( p->pgConTrk))
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,fail to SQL link pgConTrk reset", p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    sleep(10);
	  }
	}
	while( myDB_pgLinkStatus( p->pgConTrk));
	/*Prepared statements*/
	if( !p->trackingPrepared)
	{
	  ///*Prepared for subscriber tracking*/
	  //if(!retCode)
	  //{
	  //  retCode = subsTrackSqlPrepare( p->pgConTrk, subsTrackPrepare);
	  //  if( !retCode)
	  //    p->trackingPrepared = 1;
	  //}
	}
      }
      time(&t1);
      //snprintf(logStr,sizeof(logStr),"pn=%d,start computing", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      /*Read CDR content*/
      cdrFile = fopen( p->cdrFilePathProcessing, "r");
      if( cdrFile == NULL)
      {
	snprintf(logStr,sizeof(logStr),"pn=%d,ErrorAlAbrirElArchivo=%s,errno=%d", p->id, p->cdrFilePathProcessing, errno);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	localFlags.revertFile = 1;
      }
      else
      {
        /*Copy the whole file*/
	fseek( cdrFile, 0, SEEK_END);
	cdrFileLen = ftell( cdrFile);
	if( cdrContent)
	  cdrContent = realloc(cdrContent,cdrFileLen);
	else
	  cdrContent = calloc(1,cdrFileLen);
	fseek( cdrFile, 0, SEEK_SET);
	fread( cdrContent, cdrFileLen, 1, cdrFile);
	fclose(cdrFile);
	totalTagCount=0;
	pgwRecordCount=0;
	subsTrackCount=0;
	targetBerTag.level = 0;
	targetBerTag.parent = 0;
	targetBerTag.tNumber = GPRSRecord_pGWRecord;
	///*DEBUG INFORMATIONAL*/
	//debugSetLevel( DEBUG_INFORMATIONAL);
	unsigned int auxLen=cdrFileLen;
	///*Create pdp table*/
	//snprintf(pdpTableName,sizeof(pdpTableName),"pdpTable%08X%08X",(unsigned int)p->tId,pdpTableCounter++);
	//createPdpTable( p->pgCon, pdpTableName);
	///*Subscribe the new pdp table*/
	//subscribePdpTable(p->pgCon,pdpTableName);
	while(auxLen)
	{
	  /*Decode file content into asn1BerTag struct array*/
	  localTagCount = 0;
	  if( !asn1BerDecodeTarget(&localAsn1BerTag,&targetBerTag,cdrContent+cdrFileLen-auxLen,&auxLen,&localTagCount))
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,Error in asn1BerDecodeTarget",p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    localFlags.error = 1;
	    break;
	  }
	  totalTagCount += localTagCount;
	  pgwRecordCount++;
	  /*Clean pgwRecord record*/
	  tgppBerCdrCleanPGWRecordStruct(&pgwRecord);
	  bzero(&userComb,sizeof(struct _userComb));
	  /*Get pgwRecord struct from asn1Tag*/
	  if( !tgppBerCdrPgwRecordExtract( &localAsn1BerTag, &pgwRecord))
	  {
	    snprintf(logStr,sizeof(logStr),"pn=%d,Error in tgppBerCdrPgwRecordExtract",p->id);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    localFlags.error = 1;
	    break;
	  }
	  ///*User combination*/
	  //pgwRecord.extra1 = &userComb;
	  //userComb.msisdn = bcdToUInt64Reverse(pgwRecord.servedMSISDN,MSISDN_LEN);
	  //userComb.apnId = getApnId(CDRBUILTINDB_DEFAULT_APNTABLE,pgwRecord.accessPointNameNI,&apnId,&apnMutex);
	  //if( pgwRecord.recordSequenceNumber == 0)
	  //{
	  //  unsigned char ipStr[16] = {0};
	  //  ipStr[10] = 0xFF;
	  //  ipStr[11] = 0xFF;
	  //  if( !memcmp(&pgwRecord.servedPDPPDNAddress,ipStr,12))
	  //  {
	  //    userComb.flags.ipv4 = 1;
	  //    uint8_t *ipStrP=NULL;
	  //    ipStrP = (uint8_t *)&pgwRecord.servedPDPPDNAddress;
	  //    memcpy(&userComb.ip,ipStrP+12,4);
	  //  }
	  //  else
	  //  {
	  //    userComb.flags.ipv6 = 1;
	  //    userComb.ipv6 = pgwRecord.servedPDPPDNAddress;
	  //  }
	  //  userComb.time = mktime(&pgwRecord.recordOpeningTime);
	  //  userComb.imsi = bcdToUInt64Reverse(pgwRecord.servedIMSI,IMSI_LEN);
	  //  userComb.imei = bcdToUInt64Reverse(pgwRecord.servedIMEI,IMEISV_LEN);
	  //  addUserComb(&userComb);
	  //  /*Notification service*/
	  //  if( cdrDecParam.subsNotify)
	  //  {
	  //    notifyInfo = (struct _notifyInfo){{0}};
	  //    notifyInfo.flags.msisdn = 1;
	  //    notifyInfo.msisdn = userComb.msisdn;
	  //    if( !getNotifyInfoMsisdn( &notifyInfo))
	  //    {
	  //      bodyLen = snprintf(bodyStr,sizeof(notifyStr),"{'subject':'PS network registration','msg':'The subscriber %lu was connected to PS network at %s','To':[",notifyInfo.msisdn,asctime_r(&pgwRecord.recordOpeningTime,timeStr));
	  //      for( i=0;i<notifyInfo.emailCount;i++)
	  //      {
	  //        bodyLen += snprintf(bodyStr+bodyLen,sizeof(notifyStr)-bodyLen,"'%s',",notifyInfo.email[i]);
	  //      }
	  //      bodyLen += snprintf(bodyStr+bodyLen,sizeof(notifyStr)-bodyLen,"]}");
	  //      notifyLen = snprintf(notifyStr,sizeof(notifyStr),"POST /cgi-script/notifycup HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nsoapAction: \"emailNotification\"\r\nContent-Length: %u\r\n\r\n",notifySvrName,bodyLen);
	  //      notifyLen += snprintf(notifyStr+notifyLen,sizeof(notifyStr)-notifyLen,"%s",bodyStr);
	  //      if( oamGetLinkStatus(3) == OAMLINKSTATUS_CONN)
	  //      {
	  //        retCode = oamSendMsgLnk(notifyStr,notifyLen,3);
	  //        if( retCode)
	  //        {
	  //          snprintf(logStr,sizeof(logStr),"pn=%d,Error when send notification link %d, errno %d",p->id,3,retCode);
	  //          debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  //        }
	  //      }
	  //      /*Notify WEBSERVICE*/
	  //      freeNotifyInfo(&notifyInfo);
	  //    }
	  //  }
	  //}
	  ///*Subscriber DB*/
	  //if( cdrDecParam.subsDb)
	  //{
	  //  if( pgwCDRSubsDB( p->subsDbId,&pgwRecord))subscriberCount++;
	  //}
	  ///*Subscriber trace*/
	  //if( cdrDecParam.subsTrace)
	  //{
	  //  pgwCDRSubsTrc( &pgwRecord);
	  //}
	  /*Subscriber tracking*/
	  if( cdrDecParam.subsTrack)
	  {
	    /*Tracking in SQL mode*/
	    if( cdrDecParam.subsTrackSql)
	    {
	      if(!retCode)
	      {
	        //retCode = substrackToSqlPrepared( p->pgConTrk, &pgwRecord, subsTrackPrepare);
		//if(!substrk_buffer)
		//{
		//  substrk_buffer = malloc(SUBSTRK_BUFFER);
		//  substrk_buffer_len = SUBSTRK_BUFFER;
		//}
		//if((substrk_buffer_len-substrk_buffer_count)<SUBSTRK_BUFFER)
		//{
		//  substrk_buffer = realloc(substrk_buffer,substrk_buffer_len+SUBSTRK_BUFFER);
		//  substrk_buffer_len += SUBSTRK_BUFFER;
		//}
		//if( pgwRecordCount == 1)
		//{
		//  substrkMinTor = pgwRecord.recordOpeningTime;
		//  substrT = mktime(&pgwRecord.recordOpeningTime)+pgwRecord.duration;
		//  localtime_r(&substrT,&substrkMaxTor);
		//}
		//else
		//{
		//  /*Calculate min time for tracking table*/
		//  substrkTDiff = difftime(mktime(&substrkMinTor),mktime(&pgwRecord.recordOpeningTime));
		//  if(substrkTDiff>0)
		//    substrkMinTor = pgwRecord.recordOpeningTime;
		//  /*Calculate max time for tracking table*/
		//  substrkTDiff = difftime(mktime(&pgwRecord.recordOpeningTime)+pgwRecord.duration,mktime(&substrkMaxTor));
		//  if(substrkTDiff>0)
		//  {
		//    substrT = mktime(&pgwRecord.recordOpeningTime)+pgwRecord.duration;
		//    localtime_r(&substrT,&substrkMaxTor);
		//  }
		//}
		//retCode = substrackToBuffer(&pgwRecord, substrk_buffer, &substrk_buffer_len, &substrk_buffer_count, substrkTableNameDB);
		retCode = substrackToStruct(&pgwRecord, substrkDB, substrkTableNameDB);
		if(retCode < 0)
		{
		  snprintf(logStr,sizeof(logStr),"pn=%d,Error in substrackToSqlPrepared:%d",p->id,retCode);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  localFlags.revertFile = 1;
		  break;
		}
		else
		{
		  subsTrackCount += retCode;
		}
	      }
	    }
	    /*Tracking in binary file mode*/
	    if( cdrDecParam.subsTrackFile)
	    {
	      retCode = pgwCDRSubsTrk( &pgwRecord);
	      if( retCode)
	      {
		snprintf(logStr,sizeof(logStr),"pn=%d,Error in pgwCDRSubsTrk:%d",p->id,retCode);
		debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		localFlags.revertFile = 1;
		break;
	      }
	    }
	  }
	  /*PGW statistics*/
	  if( cdrDecParam.pwgStat)
	  {
	    if( cdr_pgwcdrcmb==NULL)
	      cdr_pgwcdrcmb = calloc(1,sizeof(struct _cdr_pgwcdrcmb));
	    cdr_pgwcdrcmb->tst_tm = pgwRecord.recordOpeningTime;
	    cdr_pgwcdrcmb->cc = pgwRecord.chargingCharacteristics;
	    cdr_pgwcdrcmb->dynamicAddressFlag = pgwRecord.dynamicAddressFlag;
	    cdr_pgwcdrcmb->iMSsignalingContext = pgwRecord.iMSsignalingContext;
	    cdr_pgwcdrcmb->apnSelectionMode = pgwRecord.apnSelectionMode;
	    cdr_pgwcdrcmb->chChSelectionMode = pgwRecord.chChSelectionMode;
	    /*Add serving gateway MCC*/
	    if( cdr_mcc==NULL)
	      cdr_mcc = calloc(1,sizeof(uint16_t));
	    *cdr_mcc = pgwRecord.servingNodePLMNIdentifier.mcc;
	    if( !myDB_insertRow( mccDB, cdr_mcc))
	      cdr_mcc = NULL;
	    /*Add serving gateway PLMN*/
	    if( !cdr_plmnId)
	      cdr_plmnId = calloc(1,sizeof(struct _plmnId));
	    *cdr_plmnId = pgwRecord.servingNodePLMNIdentifier;
	    if( !myDB_insertRow( plmnDB, cdr_plmnId))
	      cdr_plmnId = NULL;
	    /*Add gateway MCC*/
	    if( cdr_mcc==NULL)
	      cdr_mcc = calloc(1,sizeof(uint16_t));
	    *cdr_mcc = pgwRecord.p_GWPLMNIdentifier.mcc;
	    if( !myDB_insertRow( mccDB, cdr_mcc))
	      cdr_mcc = NULL;
	    /*Add gateway PLMN*/
	    if( !cdr_plmnId)
	      cdr_plmnId = calloc(1,sizeof(struct _plmnId));
	    *cdr_plmnId = pgwRecord.p_GWPLMNIdentifier;
	    if( !myDB_insertRow( plmnDB, cdr_plmnId))
	      cdr_plmnId = NULL;
	    /*Add APN*/
	    if(!cdr_apn)
	      cdr_apn = calloc(1,sizeof(struct _apnId));
	    strcpy(cdr_apn->apnNi,pgwRecord.accessPointNameNI);
	    cdr_apn->plmn = pgwRecord.p_GWPLMNIdentifier;
	    cdr_apn_B = myDB_insertRow( apnDB, cdr_apn);
	    if(!cdr_apn_B)
	    {
	      cdr_apn->id = apnId_id++;
	      cdr_pgwcdrcmb->apnId_id = cdr_apn->id;
	      cdr_apn = NULL;
	    }
	    else
	    {
	      cdr_pgwcdrcmb->apnId_id = cdr_apn_B->id;
	    }
	    /*Add NodeId*/
	    if(!cdr_nodeId)
	      cdr_nodeId = calloc(1,sizeof(struct _nodeId));
	    strcpy(cdr_nodeId->nodeName,pgwRecord.nodeID);
	    cdr_nodeId->plmn = pgwRecord.p_GWPLMNIdentifier;
	    if(!myDB_insertRow( nodeDB, cdr_nodeId))
	      cdr_nodeId = NULL;
	    /*Add serving gateway address*/
	    if( !snAddress)
	      snAddress = calloc(1,sizeof(struct _snaddress));
	    snAddress->plmn = pgwRecord.servingNodePLMNIdentifier;
	    snAddress->snaddress = pgwRecord.servingNodeAddress;
	    snAddress->sntype = pgwRecord.servingNodeType;
	    snAddress_B = myDB_insertRow( snAddressDB, snAddress);
	    if(!snAddress_B)
	    {
	      snAddress->id = snaddress_id++;
	      cdr_pgwcdrcmb->snaddress_id = snAddress->id;
	      snAddress = NULL;
	    }
	    else
	    {
	      cdr_pgwcdrcmb->snaddress_id = snAddress_B->id;
	    }
	    /*Add pdn gateway address*/
	    if( !gwAddress)
	      gwAddress = calloc(1,sizeof(struct _gwaddress));
	    gwAddress->plmn = pgwRecord.p_GWPLMNIdentifier;
	    gwAddress->gwaddress = pgwRecord.p_GWAddress;
	    strcpy(gwAddress->nodeId,pgwRecord.nodeID);
	    gwAddress_B = myDB_insertRow( gwAddressDB, gwAddress);
	    if(!gwAddress_B)
	    {
	      gwAddress->id = gwaddress_id++;
	      cdr_pgwcdrcmb->gwaddress_id = gwAddress->id;
	      gwAddress = NULL;
	    }
	    else
	    {
	      cdr_pgwcdrcmb->gwaddress_id = gwAddress_B->id;
	    }
	    /*Add cell identity*/
	    if(!cdr_ci)
	      cdr_ci = calloc(1,sizeof(struct _cdrCi));
	    cdr_ci->uli = pgwRecord.userLocationInformation;
	    cdr_ci->ratType = pgwRecord.rATType;
	    cdr_ci_B = myDB_insertRow( ciDB, cdr_ci);
	    if(!cdr_ci_B)
	    {
	      cdr_ci->id = cdrCi_id++;
	      cdr_pgwcdrcmb->cdrCi_id = cdr_ci->id;
	      cdr_ci = NULL;
	    }
	    else
	    {
	      cdr_pgwcdrcmb->cdrCi_id = cdr_ci_B->id;
	    }
	    /*PGW CDR combination*/
	    //cdr_pgwcdrcmbDB
	    cdr_pgwcdrcmb_B = myDB_insertRow(cdr_pgwcdrcmbDB,cdr_pgwcdrcmb);
	    if(!cdr_pgwcdrcmb_B)
	    {
	      cdr_pgwcdrcmb->id = cdr_pgwcdrcmb_id++;
	      statParams.cdr_pgwcdrcmb = cdr_pgwcdrcmb;
	      cdr_pgwcdrcmb = NULL;
	    }
	    else
	    {
	      statParams.cdr_pgwcdrcmb = cdr_pgwcdrcmb_B;
	    }
	    /*lotv stat, losd stat, pdp stat*/
	    statParams.pgwRecord = &pgwRecord;
	    statParams.coccDB = pgwCDRCoccDB;
	    statParams.coscDB = pgwCDRCoscDB;
	    statParams.pdpStatDB = pdpStatDB;
	    statParams.ciDB = ciDB;
	    statParams.lotvTablesDB = lotvTablesDB;
	    statParams.losdTablesDB = losdTablesDB;
	    statParams.pgwRecordTablesId = &cdr_pgwcdrcmb_id;
	    statParams.cdrCi_id = &cdrCi_id;
	    statParams.pgwCdrEventTablesDB = pgwcdrEventTablesDB;
	    statParams.pgwCdrCmbDB = cdr_pgwcdrcmbDB;
	    retCode = cdrPGWRecordStat( &statParams);
	    if( retCode)
	    {
	      snprintf(logStr,sizeof(logStr),"pn=%d,Error in cdrPGWRecordStat %d",p->id,retCode);
	      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      localFlags.revertFile = 1;
	      break;
	    }
	    else
	    {
	      totalCocc += pgwRecord.listOfTrafficVolumesCount;
	      totalCosc += pgwRecord.listOfServiceDataCount;
	    }
	  }
	}
      //snprintf(logStr,sizeof(logStr),"pn=%d,start tracking", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	//cdrPGWStatSQLResume( p->pgCon,coccttn,coscttn);
	/*Consolide subscriber tracking SQL mode*/
	if( cdrDecParam.subsTrack && cdrDecParam.subsTrackSql)
	{
	  //if(!retCode)
	  //{
	  //  /*Add cell identity for subscriber tracking*/
      //snprintf(logStr,sizeof(logStr),"pn=%d,start addCdrCi_substrk", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  //  myDB_rewindIterator( it_ciDB);
	  //  retCode = addCdrCi_substrk(p->pgConTrk, it_ciDB, subsTrackPrepare);
	  //}
	  //if( p->trackingPrepared)
	  if(!retCode)
	  {
	    retCode = subsTrackSqlTmpTable(p->pgConTrk, substrkTableNameIt);
	  }
	  /*BEGIN SQL tracking*/
	  if(!retCode)
	    retCode = cdrPGWStatSQLBEGIN( p->pgConTrk);
	  ///*Add apn data for subscriber tracking*/
	  //if(!retCode)
	  //{
      //snprintf(logStr,sizeof(logStr),"pn=%d,start addCdrApn_substrk", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  //  myDB_rewindIterator( it_apnDB);
  	  //  retCode = addCdrApn_substrk(p->pgConTrk, it_apnDB, subsTrackPrepare);
	  //}
	  /*Subscriber tracking temporary table*/
	  //if(!retCode)
	  //{
	  //  snprintf( subsTrackPrepare, sizeof(subsTrackPrepare), "cdr.s_%lx_%u", p->tId, substrkSeq++);
	  //  retCode = subsTrackSqlTmpTable( p->pgConTrk, subsTrackPrepare, substrkMinTor, substrkMaxTor);
	  //}
	  if(!retCode)
	  {
      //snprintf(logStr,sizeof(logStr),"pn=%d,start copyCdrSubsTrack", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    retCode = copyCdrSubsTrackTables(p->pgConTrk,it_substrkDB,substrkTableNameIt);
	    //retCode = copyCdrSubsTrack(p->pgConTrk, subsTrackPrepare, substrk_buffer, substrk_buffer_count, substrkTableNameIt);
	    //retCode = insertCdrSubsTrack(p->pgConTrk,subsTrackPrepare);
	  }
	}
      //snprintf(logStr,sizeof(logStr),"pn=%d,start pgw stas", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	/*PGW stat to SQL*/
	if( !localFlags.revertFile && !localFlags.error)
	{
	  /*PGW stat to SQL*/
	  if( cdrDecParam.pwgStat)
	  {
	    /*create LOTV tables*/
	    if(!retCode)
	    {
      //snprintf(logStr,sizeof(logStr),"pn=%d,create LOTV tables", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      myDB_rewindIterator( it_lotvTableNameDB);
	      //retCode = addLotvTables(p->pgCon,it_lotvTableNameDB);
	    }
	    /*create LOSD tables*/
	    if(!retCode)
	    {
      //snprintf(logStr,sizeof(logStr),"pn=%d,create LOSD tables", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      myDB_rewindIterator( it_losdTableNameDB);
	      //retCode = addLosdTables(p->pgCon,it_losdTableNameDB);
	    }
	    /*Insert MCC*/
	    if(!retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add MCC", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_mccDB);
//	      retCode = addCdrMcc(p->pgCon, it_mccDB);
	    }
	    /*Insert MNC*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add MNC", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_plmnDB);
//	      retCode = addCdrPlmn(p->pgCon,it_plmnDB);
	    }
	    /*Insert APN*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add CI", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_apnDB);
//	      retCode = addCdrApn(p->pgCon, it_apnDB, coccttn);
	    }
	    /*Insert NodeId*/
	    if( !retCode)
	    {
    //snprintf(logStr,sizeof(logStr),"pn=%d, add NODEID", p->id);
    //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      myDB_rewindIterator( it_nodeDB);
	      retCode = addCdrNodeId(p->pgCon,it_nodeDB);
	    }
	    /*Insert serving node address*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add snAddress", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_snAddressDB);
//	      retCode = addCdrSnAddress(p->pgCon, it_snAddressDB, coccttn);
	    }
	    /*Insert gateway address*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add gwAddress", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_gwAddressDB);
//	      retCode = addCdrGwAddress(p->pgCon, it_gwAddressDB, coccttn);
	    }
	    /*Insert cell idetity*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add CI", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_ciDB);
	      //retCode = addCdrCi(p->pgCon, it_ciDB, coccttn);
	    }
	    /*Insert PGWCDR combination*/
	    if( !retCode)
	    {
//    snprintf(logStr,sizeof(logStr),"pn=%d, add PGWCDR cmb", p->id);
//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	      myDB_rewindIterator( it_cdr_pgwcdrcmbDB);
	      //retCode = addPgwCdrCmb(p->pgCon, it_cdr_pgwcdrcmbDB, coccttn);
	    }
	    /*BEGIN SQL*/
            if(!retCode)
	    {
    //snprintf(logStr,sizeof(logStr),"pn=%d, BEGIN", p->id);
    //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      retCode = cdrPGWStatSQLBEGIN( p->pgCon);
	    }
	    /*Inserting into SQL db*/
	    if( !retCode)
	    {
	      snprintf( ttn, sizeof(ttn), "%lx_%.8x", p->tId, statSeq++);
	      /*Insert LOTV*/
	      if( !retCode)
	      {
      //snprintf(logStr,sizeof(logStr),"pn=%d, add LOTV", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		myDB_rewindIterator( it_lotvTableNameDB);
		myDB_rewindIterator( coccDbIt);
		retCode = cdrPGWCoccStatInsertSQL(coccDbIt,p->pgCon,ttn,it_lotvTableNameDB);
	      }
	      /*Insert list of service data*/
	      if( !retCode)
	      {
      //snprintf(logStr,sizeof(logStr),"pn=%d, add LOSD", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		myDB_rewindIterator( it_losdTableNameDB);
		myDB_rewindIterator( coscDbIt);
		retCode = cdrPGWCoscStatInsertSQL( coscDbIt, p->pgCon, ttn, it_losdTableNameDB);
	      }
	      /*Insert PDP context stats*/
	      if( !retCode)
	      {
      //snprintf(logStr,sizeof(logStr),"pn=%d, add PDPstat", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		myDB_rewindIterator( pdpStatDbIt);
		retCode = cdrPGWpdpStatInsertSQL( pdpStatDbIt, p->pgCon, pdpPrepareStr);
	      }
	      /*COMMIT SQL STAT*/
      //snprintf(logStr,sizeof(logStr),"pn=%d, COMMIT", p->id);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	      cdrPGWStatSQLCOMMIT( p->pgCon);
	    }
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_gwaddress",coccttn);
	    myDB_PQexec( p->pgCon, sql, NULL, 0);
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_snaddress",coccttn);
	    myDB_PQexec( p->pgCon, sql, NULL, 0);
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_apn",coccttn);
	    myDB_PQexec( p->pgCon, sql, NULL, 0);
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_ci",coccttn);
	    myDB_PQexec( p->pgCon, sql, NULL, 0);
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_pgwcdrcmb",coccttn);
	    myDB_PQexec( p->pgCon, sql, NULL, 0);
	  }
	  /*Error on insert*/
	  if( retCode)
	  {
	    localFlags.revertFile = 1;
	  /*Rollback SQL tracking*/
	    if (cdrDecParam.subsTrack && cdrDecParam.subsTrackSql)
	      myDB_PQexec( p->pgConTrk, "ROLLBACK", NULL, 0);
	  }
	}
	/*Commit SQL tracking*/
	if (cdrDecParam.subsTrack && cdrDecParam.subsTrackSql)
	{
	  cdrPGWStatSQLCOMMIT( p->pgConTrk);
	    snprintf(sql,sizeof(sql),"TRUNCATE TABLE %s_ci",subsTrackPrepare);
	    myDB_PQexec( p->pgConTrk, sql, NULL, 0);
	}
	time(&t2);
	/*CDR compact process*/
	//p->cCA->lotvRows += myDB_getIndexCount(pgwCDRCoccDB);
	//p->cCA->losdRows += myDB_getIndexCount(pgwCDRCoscDB);
	//if( p->cCA->pause)
	//{
	//  if( (localErrno = pthread_kill( p->cCA->ptid, SIGCONT)))
	//  {
	//    snprintf(logStr,sizeof(logStr),"pn=%d,Error when send SIGCONT to CDR compact process,errno=%d",p->id, localErrno);
	//    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	//  }
	//}
      }
      /*Log operation*/
      if( !localFlags.revertFile && !localFlags.error)
      {
	snprintf(logStr,sizeof(logStr),"pn=%d,FILE=%s,len=%u,tags=%u,RC=%u,Cocc=%d,lOTV=%u,Cosc=%u,lOSD=%u,pdpSC=%d,pT=%d,sC=%u,strkC=%u,mccC=%u,plmnC=%u,apnC=%u,nodeIdC=%u,snC=%u,gwC=%u,ciC=%u,pgwCdrCmb=%lu\n",p->id,p->cdrFileName,cdrFileLen,totalTagCount,pgwRecordCount,totalCocc,myDB_getIndexCount(pgwCDRCoccDB),totalCosc,myDB_getIndexCount(pgwCDRCoscDB),myDB_getIndexCount(pdpStatDB),(int)(t2-t1),subscriberCount,subsTrackCount,myDB_getIndexCount(mccDB),myDB_getIndexCount(plmnDB),myDB_getIndexCount(apnDB),myDB_getIndexCount(nodeDB),myDB_getIndexCount(snAddressDB),myDB_getIndexCount(gwAddressDB),myDB_getIndexCount(ciDB),cdr_pgwcdrcmb_id);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      /*Clean PGW stat rows*/
      myDB_dropRow( mccDB, NULL, free);
      myDB_dropRow( plmnDB, NULL, free);
      myDB_dropRow( apnDB, NULL, free);
      myDB_dropRow( nodeDB, NULL, free);
      myDB_dropRow( snAddressDB, NULL, free);
      myDB_dropRow( gwAddressDB, NULL, free);
      myDB_dropRow( ciDB, NULL, free);
      myDB_dropRow( lotvTablesDB, NULL, free);
      myDB_dropRow( losdTablesDB, NULL, free);
      myDB_dropRow( cdr_pgwcdrcmbDB, NULL, free);
      myDB_dropRow( pgwcdrEventTablesDB, NULL, free);
      myDB_dropRow( substrkTableNameDB, NULL, free);
      myDB_dropRow( substrkDB, NULL, free);
      cdrPgwRecordStatClean( pgwCDRCoccDB, pgwCDRCoscDB, pdpStatDB);
      /*Final file handle*/
      /*Revert to original file name*/
      if( localFlags.revertFile)
      {
	snprintf(logStr,sizeof(logStr),"pn=%d,FILE=%s,reverted",p->id,p->cdrFilePathReverted);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	rename(p->cdrFilePathProcessing,p->cdrFilePathReverted);
      }
      /*Error file*/
      else if( localFlags.error)
      {
	rename(p->cdrFilePathProcessing,p->cdrFilePathError);
	snprintf(logStr,sizeof(logStr),"pn=%d,FILE=%s,renamedTo=%s",p->id,p->cdrFilePathProcessing,p->cdrFilePathError);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      /*Delete file*/
      else if( cdrDecParam.delFile)
      {
	if(unlink(p->cdrFilePathProcessing))
	{
	  rename(p->cdrFilePathProcessing,cdrFilePathProcesed);
	  snprintf(logStr,sizeof(logStr),"pn=%d,ErrorWhenUnlink,FILE=%s,errno=%d,renamedTo=%s",p->id,p->cdrFilePathProcessing,errno,cdrFilePathProcesed);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
      /*Procesed file*/
      else
      {
	snprintf(cdrFilePathProcesed,sizeof(cdrFilePathProcesed),"%s.processed",p->cdrFilePath);
	rename(p->cdrFilePathProcessing,cdrFilePathProcesed);
      }
      p->busy = 0;
    }
  }
  snprintf(logStr,sizeof(logStr),"Exiting:processId=%d,threadId=0x%lX", p->id, p->tId);
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  return NULL;
}
void handler(int signum)
{
  // Empty
}
/*
 	cdrDecProcFile()
	This function match if the file found match the CDR file name pattern and assign to the first CDR idle process
	return:
		0 = success
		1 = file name does not match
		2 = no resource available
		3 = fail to wake process
*/
int cdrDecProcFile( char *dir, char *fileName, void *p)
{
  int retCode=2;
  char logStr[1024];
  int localErrno=0;
  /*Process threads*/
  struct _cdrProcess /**cdrProcessT=p,*/ *cdrProcessTA=NULL;;
  char cdrFilePattern[256]=".*.dat$";
  char cdrFilePathProcessing[1024];
  regex_t preg;
  char cdrFilePath[256]="./";
  /*File path*/
  snprintf(cdrFilePath,sizeof(cdrFilePath),"%s/%s",dir,fileName);
  /*Analize file pattern*/
  regcomp(&preg,cdrFilePattern,REG_NOSUB);
  if( !regexec(&preg, cdrFilePath, 0, NULL, 0))
  {
    /*Asign CDR file to a process*/
    for( cdrProcessTA=p; cdrProcessTA; cdrProcessTA=cdrProcessTA->next)
    {
      if( cdrProcessTA->busy)
	continue;
      /*Rename to .processing*/
      snprintf(cdrFilePathProcessing,sizeof(cdrFilePathProcessing),"%s.processing",cdrFilePath);
      rename(cdrFilePath,cdrFilePathProcessing);
      strncpy( cdrProcessTA->cdrFilePathProcessing, cdrFilePathProcessing, sizeof(cdrProcessTA->cdrFilePathProcessing));
      strncpy( cdrProcessTA->cdrFilePath, cdrFilePath, sizeof(cdrProcessTA->cdrFilePath));
      /*File name whitout dir path*/
      strncpy( cdrProcessTA->cdrFileName, fileName, sizeof(cdrProcessTA->cdrFileName));
      /*Error file path*/
      if( cdrDecParam.cdrDir[cdrDecParam.errorFileDir])
      {
        snprintf(cdrProcessTA->cdrFilePathError,sizeof(cdrProcessTA->cdrFilePathError),"%s%s.error",cdrDecParam.cdrDir[cdrDecParam.errorFileDir],fileName);
      }
      else
      {
        snprintf(cdrProcessTA->cdrFilePathError,sizeof(cdrProcessTA->cdrFilePathError),"%s.error",cdrFilePath);
      }
      /*Reverted file path*/
      if( cdrDecParam.cdrDir[cdrDecParam.revertedFileDir])
      {
        snprintf(cdrProcessTA->cdrFilePathReverted,sizeof(cdrProcessTA->cdrFilePathReverted),"%s%s",cdrDecParam.cdrDir[cdrDecParam.revertedFileDir],fileName);
      }
      else
      {
        snprintf(cdrProcessTA->cdrFilePathReverted,sizeof(cdrProcessTA->cdrFilePathReverted),"%s",cdrFilePath);
      }
      /*Wake process*/
      cdrProcessTA->busy = 1;
      if( (localErrno = pthread_kill( cdrProcessTA->tId, SIGCONT)))
      {
	cdrProcessTA->busy = 0;
	snprintf(logStr,sizeof(logStr),"Error when send SIGCONT to process %u,errno=%d", cdrProcessTA->id, localErrno);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	retCode = 3;
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"File %s asigned to process %u",cdrFilePath,cdrProcessTA->id);
	debugLogging( DEBUG_WARNING, _CDR_DECODER_LOG_NAME_, logStr);
	retCode = 0;
      }
      break;
    }
  }
  else
  {
    retCode = 1;
  }
  regfree(&preg);
  return retCode;
}
int main( int argv, char **argc)
{
  int retCode=0, auxErrno, /*retVal=0,*/ localErrno=0;
  //char *cdrDirPath=NULL;
  DIR *cdrDir=NULL;
  struct dirent *dirEntry=NULL;
  unsigned int i;
  char *cdrConfPath="./cdrDecoder.conf";
  char *logPath=NULL;
  FILE *pidFile=NULL;
  char *pidFilePath="./cdrDecoder.pid";
  char logStr[1024];
  //struct _pgCon *pgConA=NULL;
  /*Process threads*/
  struct _cdrProcess *cdrProcessT=NULL, *cdrProcessTA=NULL;
  /*Signal handler struct*/
  struct sigaction act;
  pid_t pid=0;
  debugSetLevel(DEBUG_ERROR);
  /*Command line options*/
  for(i=0; i<argv; i++)
  {
    if( strncmp( argc[i],"--one",strlen("--one"))==0)
    {
      cdrDecParam.one = 1;
    }
    if( strncmp( argc[i],"--cdrdir=",strlen("--cdrdir="))==0)
    {
      //cdrDirPath = argc[i]+strlen("--cdrdir=");
    }
    if( strncmp( argc[i],"--conf=",strlen("--conf="))==0)
    {
      cdrConfPath = argc[i]+strlen("--conf=");
    }
    if( strncmp( argc[i],"--pidFile=",strlen("--pidFile="))==0)
    {
      pidFilePath = argc[i]+strlen("--pidFile=");
    }
    if( strncmp( argc[i],"--logdir=",strlen("--logdir="))==0)
    {
      logPath = argc[i]+strlen("--logdir=");
      debugSetLogDir( logPath);
      //printf("logdir %s\n", logPath);
      //exit(0);
    }
    if( strncmp( argc[i],"--debug",strlen("--debug"))==0)
    {
      debugSetLevel(DEBUG_INFORMATIONAL);
    }
  }
  /*FORK*/
  pid = fork();
  if( pid>0)
  {
    printf("Process Id: %d\n", pid);
    pidFile = fopen( pidFilePath, "w");
    fprintf(pidFile,"%d",pid);
    fclose( pidFile);
    return 0;
  }
  else if( pid<0)
  {
    printf("ERROR FATAL: no se pudo hacer fork: errno=%d\n",errno);
  }
  unsigned int procTId=0;
  /*Local flags*/
  struct _localFlags {
    unsigned cdrFileFound:1;
    unsigned paused:1;
  } localFlags={0};
  snprintf(logStr,sizeof(logStr),"LoadingCdrDecoderService:processId=%d", getpid());
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  /*Signal handler set*/
  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGCONT, &act, NULL);
  //mySignalHandler(0);
  /*In memory DB*/
  subscriberDB = myDB_createObj( sizeof(struct _subscriber), 1000, subscriberCmpMsisdn);
  subscriber_cdr_db = myDB_createObj( sizeof(struct _subscriber), 1000, subscriberCmpMsisdn);
  /*Get CPU count*/
  globalCpuNum = sysconf(_SC_NPROCESSORS_ONLN);
  snprintf(logStr,sizeof(logStr),"Number of CPU online=%lu", globalCpuNum);
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //char **list = myListFiles(cdrDirPath);
  //if( list)
  //{
  //  for( i=0; list[i]; i++)
  //  {
  //    printf("%s\n",list[i]);
  //  }
  //}
  //return 0;
  ///*Init DB*/
  //myDBInit(0);
  /*Configure and OAM*/
  oamInsertParser("MODCDP",cdrDecoderOamParser); /*Modify cdr decoder service parameter*/
  oamInsertParser("SUBSTRK",cdrDecoderOamParser); /*Modify built-in db settings*/
  oamInsertParser("MODSTRK",cdrDecoderOamParser); /*Modify built-in db settings*/
  oamInsertParser("ADDCDST",cdrDecoderOamParser); /*Add a subscriber trace*/
  oamInsertParser("RMVCDST",cdrDecoderOamParser); /*Delete a subscriber trace*/
  oamInsertParser("ADDCDD",cdrDecoderOamParser); /*Add a CDR decoder directory*/
  oamInsertParser("RMVCDD",cdrDecoderOamParser); /*Remove a CDR decoder directory*/
  oamInsertParser("PGLINKADD",cdrDecoderOamParser); /*Create the PostgreSQL connection*/
  oamInsertParser("LSTISDN",cdrDecoderOamParser); /*List MSISDN in DB*/
  oamInsertParser("LSTUCMB",cdrDecoderOamParser); /*List user combination*/
  oamInsertParser("LSTSTRK",cdrDecoderOamParser); /*List tracking*/
  oamInsertParser("LSTCELL",cdrDecoderOamParser); /*List celltable*/
  oamInsertParser("LSTAPN",cdrDecoderOamParser); /*List APN*/
  oamInsertParser("ADDSNTF",cdrDecoderOamParser); /*Add notification*/
  oamInsertParser("LSTSNTF",cdrDecoderOamParser); /*List notification*/
  oamInsertParser("DELSNTF",cdrDecoderOamParser); /*Del notification*/
    snprintf(logStr,sizeof(logStr),"Starting OAM service");
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  if( (retCode=oamInit( cdrConfPath, 1))) /*OamServerInit*/
  {
    snprintf(logStr,sizeof(logStr),"ErrorEnOamInit=%d",retCode);
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //return -1;
  }
    snprintf(logStr,sizeof(logStr),"OAM service started");
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  /*Subscriber SQL tracking schema*/
  if ( !cdrDecParam.statSqlSchema)
  {
    #define DEFAULTTRKSCHEMA "public"
    cdrDecParam.statSqlSchema = calloc(1,strlen(DEFAULTTRKSCHEMA)+1);
    strcpy(cdrDecParam.statSqlSchema,DEFAULTTRKSCHEMA);
  }
  ///*In file DB*/
  //myDbFile_setLogName(_CDR_DECODER_LOG_NAME_);
  //retCode = cdrBuiltInDb_init(NULL,0);
  //snprintf(logStr,sizeof(logStr),"Init built-in DB: %d", retCode);
  //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //snprintf(logStr,sizeof(logStr),"CellId: %u", cellId);
  //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //snprintf(logStr,sizeof(logStr),"ApnId: %u", apnId);
  //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //if( retCode)
  //  return retCode;
  /*SQL compact process*/
  //cdrDecParam.decoderOn = 0;
  struct _cdrCompactArg cCA={0}, cCB={0}, cCC={0}, cCD={0}, cCE={0}, cCF={0}, cCG={0}, cCH={0};
  /*Compact process for LOTV*/
  cCA.sleep = 1200;
  cCA.run = 1;
  cCA.maintainLotv = 1;
  cCA.id = procTId++;
  //snprintf(logStr,sizeof(logStr),"Starting compact service");
  //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  if( pthread_create( &cCA.ptid, NULL, cdrCompact, &cCA))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for LOSD*/
  cCB.sleep = 60;
  cCB.run = 1;
  cCB.maintainLosdTmp = 1;
  cCB.id = procTId++;
  if( pthread_create( &cCB.ptid, NULL, cdrCompact, &cCB))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for subscriber tracking*/
  cCC.sleep = 4200;
  cCC.run = 1;
  cCC.maintainSubsTrk = 1;
  cCC.id = procTId++;
  if( pthread_create( &cCC.ptid, NULL, cdrCompact, &cCC))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for pdpstat*/
  cCD.sleep = 3600;
  cCD.run = 1;
  cCD.maintainPdpstat = 1;
  cCD.id = procTId++;
  if( pthread_create( &cCD.ptid, NULL, cdrCompact, &cCD))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for lotvtmp*/
  cCE.sleep = 60;
  cCE.run = 1;
  cCE.maintainLotvTmp = 1;
  cCE.id = procTId++;
  if( pthread_create( &cCE.ptid, NULL, cdrCompact, &cCE))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for lotvmean*/
  cCF.sleep = 1;
  cCF.run = 1;
  cCF.maintainLotvMean = 1;
  cCF.id = procTId++;
  if( pthread_create( &cCF.ptid, NULL, cdrCompact, &cCF))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for LOSD*/
  cCG.sleep = 1200;
  cCG.run = 1;
  cCG.maintainLosd = 1;
  cCG.id = procTId++;
  if( pthread_create( &cCG.ptid, NULL, cdrCompact, &cCG))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  /*Compact process for LOSD mean*/
  cCH.sleep = 1200;
  cCH.run = 1;
  cCH.maintainLosdMean = 1;
  cCH.id = procTId++;
  if( pthread_create( &cCH.ptid, NULL, cdrCompact, &cCH))
  {
    /*Error when try to create new thread*/
    snprintf(logStr,sizeof(logStr),"Error when try to create compact SQL thread,description:%s",strerror(errno));
    printf("%s\n",logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  //snprintf(logStr,sizeof(logStr),"CDR compact routine started");
  //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //printf("%s\n",logStr);
  /*CDR decoder threads*/
  //for(i=0,pgConA=globalPgCon;(i<globalCpuNum)&&pgConA;i++,pgConA=pgConA->next)
    snprintf(logStr,sizeof(logStr),"Starting processor services");
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  for(i=0;(i<globalCpuNum);i++)
  {
    cdrProcessTA = calloc( 1, sizeof(struct _cdrProcess));
    if( cdrProcessTA)
    {
      cdrProcessTA->run = 1;
      cdrProcessTA->id = procTId++;
      cdrProcessTA->subsDbId = subscriberDB;
      //cdrProcessTA->pgCon = pgConA;
      //cdrProcessTA->pgConStat = myDB_selectPgconByGroup(cdrDecParam.statSqlGroup);
      //if( !cdrProcessTA->pgCon)
      //{
      //  snprintf(logStr,sizeof(logStr),"No SQL resource available, proc=%d",i);
      //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      //}
      cdrProcessTA->cCA = &cCA;
      cdrProcessTA->next = cdrProcessT;
      cdrProcessT = cdrProcessTA;
      if( pthread_create( &cdrProcessTA->tId, NULL, cdrProcess, cdrProcessTA))
      {
        /*Error when try to create new thread*/
	snprintf(logStr,sizeof(logStr),"Error when try to create thread,num=%d,description:%s",i,strerror(errno));
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        CPU_ZERO(&cdrProcessTA->cpuSet);
	CPU_SET(i,&cdrProcessTA->cpuSet);
        if((localErrno = pthread_setaffinity_np( cdrProcessTA->tId, sizeof(cpu_set_t), &cdrProcessTA->cpuSet)))
	{
	  /*Error when try to set CPU affinity*/
	  snprintf(logStr,sizeof(logStr),"Error when try to set CPU affinity,num=%d,description:%s",i,strerror(localErrno));
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
  }
  /*Infinite loop*/
  while(1)
  {
    //usleep(1000000);
    /*Service play/pause*/
    if( !cdrDecParam.decoderOn)
    {
      snprintf(logStr,sizeof(logStr),"Main process halt");
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      localFlags.paused = 1;
    }
    while(!cdrDecParam.decoderOn) sleep(1);
    if(localFlags.paused)
    {
      snprintf(logStr,sizeof(logStr),"Main process run");
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      localFlags.paused=0;
    }

    for( i=0; i<MAXCDRDIRCOUNT; i++)
    {
      localFlags.cdrFileFound = 0;
      if(!cdrDecParam.cdrDir[i])
        continue;
      /*Open CDR repo*/
      cdrDir = opendir(cdrDecParam.cdrDir[i]);
      if( cdrDir==NULL)
      {
	snprintf(logStr,sizeof(logStr),"Error opendir=%s,errno=%d", cdrDecParam.cdrDir[i], errno);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	sleep(1);
	continue;
      }
      auxErrno = errno;
      /*List files in directory*/
      while( (dirEntry = readdir(cdrDir)) && cdrDecParam.decoderOn)
      {
	/*Clear counters*/
	if( dirEntry->d_type != DT_REG)
	{
	  continue;
	}
	/*Process file*/
	if( !cdrDecProcFile(cdrDecParam.cdrDir[i],dirEntry->d_name,cdrProcessT))
	{
	  localFlags.cdrFileFound = 1;
	}
	auxErrno = errno;
	if( cdrDecParam.one)
	  break;
      }
      if( auxErrno != errno)
      {
	snprintf(logStr,sizeof(logStr),"Error readdir %s, errno %d\n", cdrDecParam.cdrDir[i], errno);
	retCode = -1;
      }
      /*Close dir*/
      closedir(cdrDir);
      if( cdrDecParam.one)
	break;
      /*If cdr files was processed, then continue listing directory*/
      if( !localFlags.cdrFileFound)
	sleep(1);
      /*Event based processing*/
      //myInodeEvent(1,&cdrDirPath,cdrDecProcFile,cdrProcessT,1000*60);/*One minute timeout*/
    }
    if( cdrDecParam.one)
      break;
  }
  return retCode;
}
