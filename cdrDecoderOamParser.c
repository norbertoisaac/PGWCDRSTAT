#define __USE_XOPEN
#define _GNU_SOURCE
#include <time.h>
#include "tc-mySubscriber.h"
#include "cdrDecoder.h"
#include "string.h"
#include <stdlib.h>
#include "tc-misc.h"
#include "stdio.h"
#include "tc-mydebug.h"
//#define _XOPEN_SOURCE
uint64_t *subsTrace;
unsigned char subsTraceCount=0;
struct _cdrDecParam cdrDecParam={0};
/*
  MODCDP  Modify cdr decoder service parameter
    MSTRK: maintain subscribre tracking tables - Pos processing
  ADDCDST Add a subscriber trace
  RMVCDST Delete a subscriber trace
  ADDCDD  Add a CDR decoder directory
  RMVCDD  Remove a CDR decoder directory
  PGLINKADD Create the PostgreSQL connection
*/
int cdrDecoderOamParser( struct _oamCommand *oamCmd, char *returnMsg, unsigned int msgLen)
{
  int retCode=0;
  unsigned int i=0,j;
  char logStr[1024];
  struct {
    unsigned subsGetUli:1;
    unsigned subsGetRi:1;
    unsigned subsGetPs:1;
    unsigned fromTm:1;
    unsigned toTm:1;
  }localFlags={0};
  char timeStr[20];
  struct _subscriber *subscriber=NULL;
  uint64_t auxUInt64=0,msisdn=0,imsi=0,imei=0;
  uint32_t auxUInt32=0;
  int auxInt=0;
  time_t auxTime=0, t=0;
  struct tm fromTM={0}, toTM={0};
  struct _notifyInfo notifyInfo={{0}};
  char *cdrDirPath=NULL;
  unsigned int cdrDirNo=0;

  if( strcmp(oamCmd->cmd,"MODCDP")==0)
  {
    for(i=0;i<oamCmd->oamCmdAVCount;i++)
    {
      if(strcmp(oamCmd->oamCmdAV[i].attr,"ERRDIR")==0)
      {
        if( (atoi(oamCmd->oamCmdAV[i].val) > 0) && (atoi(oamCmd->oamCmdAV[i].val)<=MAXCDRDIRCOUNT))
	{
	  cdrDecParam.errorFileDir = atoi(oamCmd->oamCmdAV[i].val) - 1;
	}
      }
      if(strcmp(oamCmd->oamCmdAV[i].attr,"RVTDIR")==0)
      {
        if( (atoi(oamCmd->oamCmdAV[i].val) > 0) && (atoi(oamCmd->oamCmdAV[i].val)<=MAXCDRDIRCOUNT))
	{
	  cdrDecParam.revertedFileDir = atoi(oamCmd->oamCmdAV[i].val) - 1;
	}
      }
      if(strcmp(oamCmd->oamCmdAV[i].attr,"MSTRK")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.maintainSubstrk = 1;
	else
	  cdrDecParam.maintainSubstrk = 0;
      }
      if(strcmp(oamCmd->oamCmdAV[i].attr,"DECON")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.decoderOn = 1;
	else
	  cdrDecParam.decoderOn = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"PGWSTAT")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.pwgStat = 1;
	else
	  cdrDecParam.pwgStat = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSTR")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsTrace = 1;
	else
	  cdrDecParam.subsTrace = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSTRC")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsTrace = 1;
	else
	  cdrDecParam.subsTrace = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSTRK")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsTrack = 1;
	else
	  cdrDecParam.subsTrack = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"ALLSUBSTR")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.allSubsTrace = 1;
	else
	  cdrDecParam.allSubsTrace = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSDB")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsDb = 1;
	else
	  cdrDecParam.subsDb = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"DELFILE")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.delFile = 1;
	else
	  cdrDecParam.delFile = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SNOTF")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsNotify = 1;
	else
	  cdrDecParam.subsNotify = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SQLGROUP")==0)
      {
	cdrDecParam.statSqlGroup = atoi(oamCmd->oamCmdAV[i].val);
      }
    }
  }
  else if( strcmp(oamCmd->cmd,"ADDCDST")==0)
  {
    for(i=0;i<oamCmd->oamCmdAVCount;i++)
    {
      if(strcmp(oamCmd->oamCmdAV[i].attr,"MSISDN")==0)
      {
	auxUInt64 = strToUInt64(oamCmd->oamCmdAV[i].val);
        for(j=0;j<subsTraceCount;j++)
	{
	  if( auxUInt64 == subsTrace[j])
	  {
	    snprintf(returnMsg,msgLen,"Error:SubscriberAlreadyExist");
	    retCode = -1;
	    break;
	  }
	  else if( !subsTrace[j])
	  {
	    subsTrace[j] = strToUInt64(oamCmd->oamCmdAV[i].val);
	    snprintf(logStr,sizeof(logStr),"SubscriberTraceAdded,MSISDN=%lu\n",subsTrace[j]);
	    debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
	    snprintf(returnMsg,msgLen,"SubscriberTraceAdded,MSISDN=%lu\n",subsTrace[j]);
	    subsTraceCount++;
	    break;
	  }
	}
	if( j==subsTraceCount)
	{
	  subsTraceCount++;
	  subsTrace = realloc(subsTrace,subsTraceCount*sizeof(uint64_t));
	  subsTrace[j] = strToUInt64(oamCmd->oamCmdAV[i].val);
	  snprintf(logStr,sizeof(logStr),"SubscriberTraceAdded,MSISDN=%lu\n",subsTrace[j]);
	  debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
	}
      }
    }
  }
  else if( strcmp(oamCmd->cmd,"RMVCDST")==0)
  {
    for(i=0;i<oamCmd->oamCmdAVCount;i++)
    {
      if(strcmp(oamCmd->oamCmdAV[i].attr,"MSISDN")==0)
      {
        //auxUInt64 = atoll(oamCmd->oamCmdAV[i].val);
        auxUInt64 = strToUInt64(oamCmd->oamCmdAV[i].val);
        for(j=0;j<subsTraceCount;j++)
	{
	  if( subsTrace[j] == auxUInt64)
	  {
	    snprintf(logStr,sizeof(logStr),"SubscriberTraceDeleted,MSISDN=%lu\n",subsTrace[j]);
	    debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
	    //subsTrace[j] = 0;
	    subsTrace[j] = subsTrace[subsTraceCount-1];
	    subsTrace[subsTraceCount-1] = 0;
	    break;
	  }
	}
	if( j==subsTraceCount)
	{
	  snprintf(returnMsg,msgLen,"MSISDN not found");
	  retCode = -1;
	}
	else
	{
	  if( subsTraceCount)
	    subsTraceCount--;
	}
      }
    }
  }
  /*Add cdr directory*/
  else if( strcmp(oamCmd->cmd,"ADDCDD")==0)
  {
    for(i=0;i<oamCmd->oamCmdAVCount;i++)
    {
      if(strcmp(oamCmd->oamCmdAV[i].attr,"DIR")==0)
      {
        if( strlen(oamCmd->oamCmdAV[i].val))
	{
	  cdrDirPath = oamCmd->oamCmdAV[i].val;
	}
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"DNO")==0)
      {
        cdrDirNo = atoi(oamCmd->oamCmdAV[i].val);
      }
    }
    if( cdrDirNo && (cdrDirNo<=MAXCDRDIRCOUNT) && cdrDirPath)
    {
      cdrDirNo--;
      cdrDecParam.cdrDir[cdrDirNo] = calloc(1,strlen(cdrDirPath)+1);
      strcpy(cdrDecParam.cdrDir[cdrDirNo],cdrDirPath);
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"Error adding cdr dir: number=%d, path=%s",cdrDirNo,cdrDirPath);
      debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
      retCode = -1;
    }
  }
  else if( strcmp(oamCmd->cmd,"RMVCDD")==0)
  {
  }
  /*Get subscriber*/
  else if(strcmp(oamCmd->cmd,"GETSUBS")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "MSISDN")==0)
      {
	auxUInt64 = strToUInt64(oamCmd->oamCmdAV[i].val);
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "ULI")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  localFlags.subsGetUli = 1;
	else
	  localFlags.subsGetUli = 0;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "RI")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  localFlags.subsGetRi = 1;
	else
	  localFlags.subsGetRi = 0;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "PS")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  localFlags.subsGetPs = 1;
	else
	  localFlags.subsGetPs = 0;
      }
    }
    /*Search susbcriber*/
    if( auxUInt64)
    {
      //subscriber = myDBSubscriberSearchUint( auxUInt64, SubsId_typeMSISDN, &auxInt);
      subscriber = subscriberQuery( subscriberDB, auxUInt64, SubsId_typeMSISDN);
      if( subscriber)
      {
        snprintf(returnMsg,msgLen,"subscriber:{lastReport:'%s',msisdn:%lu,imsi:%lu,imei:%lu,rATT:%d,tZ:%d",
	  myStrTime_r( timeStr, sizeof(timeStr), subscriber->lastReport),
	  auxUInt64,
	  subscriber->imsi,
	  subscriber->imei,
	  subscriber->rATType,
	  subscriber->mSTimeZone
	  );
	/*UserLocationInformation Data*/
	if( localFlags.subsGetUli)
	{
	  auxInt = strlen(returnMsg);
	  snprintf(returnMsg+auxInt,msgLen-auxInt,",uli:{lac:%u,ci:%u,mcc:%u,mnc:%u,eci:%u,eciMcc:%u,eciMnc:%u}",
	    subscriber->uli.uli.lac,
	    subscriber->uli.uli.ci,
	    subscriber->uli.uli.mcc,
	    subscriber->uli.uli.mnc,
	    subscriber->uli.uli.eci,
	    subscriber->uli.uli.eciMcc,
	    subscriber->uli.uli.eciMnc
	    );
	}
	///*Routing Information Data*/
	//if( localFlags.subsGetRi)
	//{
	//  if( subscriber->ri)
	//  {
	//    auxInt = strlen(returnMsg);
	//    snprintf(returnMsg+auxInt,msgLen-auxInt,",ri:{}");
	//  }
	//}
	///*Ps information data*/
	//if( localFlags.subsGetPs)
	//{
	//  if( subscriber->psCharging)
	//  {
	//    auxInt = strlen(returnMsg);
	//    snprintf(returnMsg+auxInt,msgLen-auxInt,",ps:{}");
	//  }
	//}
	auxInt = strlen(returnMsg);
        snprintf(returnMsg+auxInt,msgLen-auxInt,"}");
      }
      else
      {
        snprintf(returnMsg,msgLen,"subscriber:{lastReport:'',msisdn:,imsi:,imei:}");
	retCode = -1;
      }
    }
  }
  else if(strcmp(oamCmd->cmd,"MODSUBS")==0)
  {
  }
  else if(strcmp(oamCmd->cmd,"PGLINKADD")==0)
  {
    //printf("Before SQL\n");
    retCode = myDBOamParser(oamCmd, logStr, sizeof(logStr));
    //printf("After SQL, %d\n",retCode);
    //cdrCompactPgCon = myDB_oamCreatePgCon( oamCmd, 1, logStr, sizeof(logStr));
  }
  else if(strcmp(oamCmd->cmd,"MODSTRK")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "DIR")==0)
      {
	strncpy(cdrDbDir,oamCmd->oamCmdAV[i].val,MYDBFILE_MAXTABLEDIRLEN);
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSTRKSQL")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsTrackSql = 1;
	else
	  cdrDecParam.subsTrackSql = 0;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SQLSCH")==0)
      {
        cdrDecParam.statSqlSchema = calloc(1,strlen(oamCmd->oamCmdAV[i].val)+1);
	strcpy(cdrDecParam.statSqlSchema,oamCmd->oamCmdAV[i].val);
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SUBSTRKFILE")==0)
      {
        if( atoi(oamCmd->oamCmdAV[i].val))
	  cdrDecParam.subsTrackFile = 1;
	else
	  cdrDecParam.subsTrackFile = 0;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "STLP")==0)
      {
        auxTime = atoi(oamCmd->oamCmdAV[i].val);
	if( auxTime)
	  cdrDecParam.strkFileLen = auxTime*24*60*60;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "STLPSQL")==0)
      {
        auxTime = atoi(oamCmd->oamCmdAV[i].val);
	if( auxTime)
	  cdrDecParam.strkSqlLen = auxTime*24*60*60;
      }
      else if(strcmp(oamCmd->oamCmdAV[i].attr,"SQLGROUP")==0)
      {
	cdrDecParam.strkSqlGroup = atoi(oamCmd->oamCmdAV[i].val);
      }
    }
  }
  else if(strcmp(oamCmd->cmd,"LSTISDN")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "ISDN")==0)
      {
	auxUInt64 = strToUInt64(oamCmd->oamCmdAV[i].val);
	if( auxUInt64)
	{
	  //printf(" strToUInt64 %lu\n",auxUInt64);
	  auxUInt32 = querySubsId(CDRBUILTINDB_DEFAULT_MSISDNTABLE,auxUInt64,&msisdn_mut);
	  if( auxUInt32)
	  {
	    snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'%lu','id':'%u'}",auxUInt64,auxUInt32);
	  }
	  else
	  {
	    snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'%lu','id':'-1'}",auxUInt64);
	  }
	}
	else
	{
	  snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'%lu','id':'-1'}",auxUInt64);
	}
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "ID")==0)
      {
        auxUInt32 = atoi(oamCmd->oamCmdAV[i].val);
	if( auxUInt32)
	{
	  auxUInt64 = querySubsMsisdn(CDRBUILTINDB_DEFAULT_MSISDNTABLE,auxUInt32,&msisdn_mut);
	  if( auxUInt64)
	  {
	    snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'%lu','id':'%u'}",auxUInt64,auxUInt32);
	  }
	  else
	  {
	    snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'-1','id':'%u'}",auxUInt32);
	  }
	}
	else
	{
	  snprintf(returnMsg,msgLen,"'subscriber':{'msisdn':'-1','id':'-1'}");
	}
      }
      else
      {
      }
    }
  }
  else if(strcmp(oamCmd->cmd,"LSTUCMB")==0)
  {
    t = 0;
    localtime_r(&t,&fromTM);
    time(&t);
    localtime_r(&t,&toTM);
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "ISDN")==0)
	msisdn = strToUInt64(oamCmd->oamCmdAV[i].val);
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "IMEI")==0)
	imei = strToUInt64(oamCmd->oamCmdAV[i].val);
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "IMSI")==0)
	imsi = strToUInt64(oamCmd->oamCmdAV[i].val);
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "FROM")==0)
      {
        fromTM.tm_mday = 1;
        fromTM.tm_hour = 0;
	//fromTM = (struct tm){0};
        if(!strptime(oamCmd->oamCmdAV[i].val,"%Y-%m",&fromTM))
	  retCode = -1;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "TO")==0)
      {
        if(!strptime(oamCmd->oamCmdAV[i].val,"%Y-%m",&toTM))
	  retCode = -1;
      }
    }
    queryCmb(oamCmd,msisdn,imsi,imei,&fromTM,&toTM);
  }
  else if(strcmp(oamCmd->cmd,"LSTSTRK")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "ISDN")==0)
      {
	msisdn = strToUInt64(oamCmd->oamCmdAV[i].val);
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "FROM")==0)
      {
        localFlags.fromTm = 1;
        if(!strptime(oamCmd->oamCmdAV[i].val,"%Y-%m-%d %H:%M:%S",&fromTM))
	  retCode = -1;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "TO")==0)
      {
        localFlags.toTm = 1;
        if(!strptime(oamCmd->oamCmdAV[i].val,"%Y-%m-%d %H:%M:%S",&toTM))
	  retCode = -1;
      }
    }
    if(!retCode)
    {
      if(!localFlags.fromTm)
      {
	time(&t);
	t -= cdrDecParam.strkFileLen;
	localtime_r(&t,&fromTM);
      }
      if(!localFlags.toTm)
      {
	time(&t);
	localtime_r(&t,&toTM);
      }
      retCode = getSubsTrack(oamCmd,msisdn,&fromTM,&toTM);
    }
  }
  else if(strcmp(oamCmd->cmd,"LSTCELL")==0)
  {
    queryCellTable(CDRBUILTINDB_DEFAULT_CELLTABLE,oamCmd);
  }
  else if(strcmp(oamCmd->cmd,"LSTAPN")==0)
  {
    queryApnTable(CDRBUILTINDB_DEFAULT_APNTABLE,oamCmd);
  }
  else if(strcmp(oamCmd->cmd,"ADDSNTF")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "ISDN")==0)
      {
	notifyInfo.msisdn = strToUInt64(oamCmd->oamCmdAV[i].val);
	notifyInfo.flags.msisdn = 1;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "IMEI")==0)
      {
	notifyInfo.imei = strToUInt64(oamCmd->oamCmdAV[i].val);
	notifyInfo.flags.imei = 1;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "EMAIL")==0)
      {
	notifyInfo.email = realloc(notifyInfo.email,(notifyInfo.emailCount+1)*sizeof(char *));
	notifyInfo.email[notifyInfo.emailCount] = calloc(1,1+strlen(oamCmd->oamCmdAV[i].val));
	strcpy( notifyInfo.email[notifyInfo.emailCount], oamCmd->oamCmdAV[i].val);
        notifyInfo.emailCount++;
      }
    }
    retCode = addNotifySubscriber( &notifyInfo);
    if( retCode)
    {
      snprintf(returnMsg,msgLen,"Error on insert");
    }
    freeNotifyInfo( &notifyInfo);
  }
  else if(strcmp(oamCmd->cmd,"DELSNTF")==0)
  {
    for( i=0; i<oamCmd->oamCmdAVCount; i++)
    {
      if( strcmp(oamCmd->oamCmdAV[i].attr, "ISDN")==0)
      {
	notifyInfo.msisdn = strToUInt64(oamCmd->oamCmdAV[i].val);
	notifyInfo.flags.msisdn = 1;
      }
      else if( strcmp(oamCmd->oamCmdAV[i].attr, "IMEI")==0)
      {
	notifyInfo.imei = strToUInt64(oamCmd->oamCmdAV[i].val);
	notifyInfo.flags.imei = 1;
      }
    }
    retCode = delNotifySubscriber( &notifyInfo);
    if( retCode)
    {
      snprintf(returnMsg,msgLen,"Error on delete");
    }
  }
  else if(strcmp(oamCmd->cmd,"LSTSNTF")==0)
  {
    listNotifyInfo( oamCmd);
  }
  return retCode;
}
