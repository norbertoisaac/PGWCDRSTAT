#include "stdio.h"
#include "time.h"
#include "tc-mydb.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include <unistd.h>
#include "cdrDecoder.h"
#include <arpa/inet.h>

/*
	Object table model
	byte 0: payload len
	byte 1-4: time
	byte 5-8: msisdnId
	byte 9: imsi len
	byte 10-m: imsi
	byte m+1: imei len
	byte m+2-t: imei
*/
void *createUSerCombTableObj( struct _userComb *userComb)
{
  void *obj=NULL, *objAux=NULL;
  uint8_t len=0, imsiLen=0, imeiLen=0, msisdnLen=0;
  imsiLen = numBytesOcuped(userComb->imsi);
  imeiLen = numBytesOcuped(userComb->imei);
  msisdnLen = numBytesOcuped(userComb->msisdn);
  len = 4+ 1+ 1+msisdnLen+ 1+imsiLen+ 1+imeiLen+ 2+ (userComb->flags.ipv4? 4:0) + (userComb->flags.ipv6? 16:0);
  obj = calloc(1,len+1);
  if( obj)
  {
    objAux = obj;
    /*Copy payload len*/
    memcpy(objAux,&len,1);
    objAux += 1;
    /*Copy time*/
    memcpy(objAux,&userComb->time,4);
    objAux += 4;
    /*Copy flags*/
    memcpy(objAux,&userComb->flags,1);
    objAux += 1;
    /*Copy msisdn len*/
    memcpy(objAux,&msisdnLen,1);
    objAux += 1;
    /*Copy msisdn*/
    memcpy(objAux,&userComb->msisdn,msisdnLen);
    objAux += msisdnLen;
    /*Copy imsi len*/
    memcpy(objAux,&imsiLen,1);
    objAux += 1;
    /*Copy imsi*/
    memcpy(objAux,&userComb->imsi,imsiLen);
    objAux += imsiLen;
    /*Copy imei len*/
    memcpy(objAux,&imeiLen,1);
    objAux += 1;
    /*Copy imei*/
    memcpy(objAux,&userComb->imei,imeiLen);
    objAux += imeiLen;
    /*Copy apnId*/
    memcpy(objAux,&userComb->apnId,2);
    objAux += 2;
    /*Copy IPv4*/
    if( userComb->flags.ipv4)
    {
      memcpy(objAux,&userComb->ip,4);
      objAux += 4;
    }
    /*Copy IPv6*/
    if( userComb->flags.ipv6)
    {
      memcpy(objAux,&userComb->ipv6,16);
      objAux += 16;
    }
  }
  return obj;
}
int getUserCombFromObj( struct _userComb *userComb, void *obj)
{
  int retCode=0;
  uint8_t len=0;
  obj += 1;
  /*Copy time*/
  memcpy(&userComb->time,obj,4);
  obj += 4;
  /*Copy flags*/
  memcpy(&userComb->flags,obj,1);
  obj += 1;
  /*Copy msisdn len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy msisdn*/
  memcpy(&userComb->msisdn,obj,len);
  obj += len;
  /*Imsi len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy imsi*/
  memcpy(&userComb->imsi,obj,len);
  obj += len;
  /*Imei len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy imei*/
  memcpy(&userComb->imei,obj,len);
  obj += len;
  /*Copy Apn*/
  memcpy(&userComb->apnId,obj,2);
  obj += 2;
  /*Copy IPv4*/
  if( userComb->flags.ipv4)
  {
    memcpy(&userComb->ip,obj,4);
    obj += 4;
  }
  /*Copy IPv6*/
  if( userComb->flags.ipv6)
  {
    memcpy(&userComb->ipv6,obj,16);
    obj += 16;
  }
  return retCode;
}
/*Comparation function*/
int cmp_userCombTable(void *pA, void *pB)
{
  int retCode=0;
  struct _userComb userCombA={0};
  struct _userComb userCombB={0};
  getUserCombFromObj( &userCombA, pA);
  getUserCombFromObj( &userCombB, pB);
  /*Compare MSISDN*/
  if( userCombA.msisdn > userCombB.msisdn)
    retCode = 1;
  else if( userCombA.msisdn < userCombB.msisdn)
    retCode = -1;
  else
  {
    /*Compare IMEI*/
    if( userCombA.imei > userCombB.imei)
      retCode = 1;
    else if( userCombA.imei < userCombB.imei)
      retCode = -1;
    else
    {
      /*Compare IMSI*/
      if( userCombA.imsi > userCombB.imsi)
	retCode = 1;
      else if( userCombA.imsi < userCombB.imsi)
	retCode = -1;
    }
  }
  //if( !retCode)
  //  printf("%u,%u %lu,%lu %lu,%lu retCode:%d\n",userCombA.msisdnId,userCombB.msisdnId,userCombA.imei,userCombB.imei,userCombA.imsi,userCombB.imsi,retCode);
  return retCode;
}
int addUserComb( struct _userComb *userComb)
{
  int retCode=0;
  struct _myDbFile_createTableAttr args={0};
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  char tName[128];
  struct tm tM;
  tuple.obj = createUSerCombTableObj( userComb);
  if( tuple.obj)
  {
    localtime_r(&userComb->time,&tM);
    strftime(tName,sizeof(tName),"userh_%Y%m",&tM);
    do
    {
      res = myDbFile_insert( tName,&tuple);
      if( res)
      {
	if( (res->status == MYDBFILE_RES_FATALERROR) && (res->errorCode == MYDBFILE_ERROR_TABLENOTFOUND))
	{
	  //printf( "MYDBFILE_ERROR_TABLENOTFOUND %s\n",tableName);
	  args.fixedRowLen = 0;
	  args.inFile = 1;
	  //args.inMemory = 1;
	  args.rowLenOctets = sizeof(uint8_t);
	  args.dbId = cdrDb_id;
	  strcpy(args.name,tName);
	  strcpy(args.dir,cdrDbDir);
	  retCode = myDbFile_createTable( &args);
	  if( retCode >= 0)
	    retCode = MYDBFILE_ERROR_TABLENOTFOUND;
	}
	else
	  retCode = res->errorCode;
	/*Clear res*/
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
  return retCode;
}

int cmp_cmbCust(void *pA, void *pB)
{
  int retCode=0;
  struct _userComb userCombA={0};
  struct _userComb userCombB={0};
  getUserCombFromObj( &userCombA, pA);
  getUserCombFromObj( &userCombB, pB);
  /*Compare MSISDN*/
  if(  userCombA.msisdn)
  {
    if( userCombA.msisdn != userCombB.msisdn)
      retCode = 1;
  }
  else if(  userCombA.imei)
  {
    if( userCombA.imei != userCombB.imei)
      retCode = 1;
  }
  else if(  userCombA.imsi)
  {
    if( userCombA.imsi != userCombB.imsi)
      retCode = 1;
  }
  else
    retCode = -1;
  //printf( "%lu %lu,%lu,%lu, %lu,%lu,%lu\n", userCombB.time, userCombA.msisdn,userCombA.imei,userCombA.imsi,userCombB.msisdn,userCombB.imei,userCombB.imsi);
  return retCode;
}
/*
	Query user combination for O&M purposes
*/
int queryCmb( struct _oamCommand *oamCmd, uint64_t msisdn, uint64_t imsi, uint64_t imei, struct tm *fromTm, struct tm *toTm)
{
  int retCode=0;
  struct _myDbFile_tuple tuple={0}, *tupleP=NULL;
  struct _myDbFile_res *res=NULL;
  struct _userComb userComb={0};
  void *obj=NULL;
  char msgStr[256];
  char timeStr[32];
  char tName[32];
  struct tm tM;
  time_t t, toT, fromT;
  uint16_t apnid=0;
  char apn[128];
  char ipStr[128];
  userComb.msisdn = msisdn;
  userComb.imsi = imsi;
  userComb.imei = imei;
  snprintf(msgStr,sizeof(msgStr),"changeTime,msisdn,imei,imsi,apn,ip\r\n");
  oamSendMsg( msgStr,strlen(msgStr), oamCmd->procNo);
  obj = createUSerCombTableObj( &userComb);
  if( obj)
  {
    tuple.obj = obj;
    time( &t);
    fromT = mktime(fromTm);
    toT = mktime(toTm);
    //printf("t %d, fromT %d, toT %d, %s\n",t,fromT,toT,asctime(fromTm));
    while( fromT<=toT)
    {
      strftime(tName,sizeof(tName),"userh_%Y%m",fromTm);
      //printf("%s \n",tName);
      res = myDbFile_queryCust( tName,&tuple,cmp_cmbCust);
      if( res)
      {
	if( res->status==MYDBFILE_RES_TUPLESOK)
	{
	  for(tupleP=res->objs;tupleP;tupleP=tupleP->next)
	  {
	    if( tupleP->obj)
	    {
	      userComb.time = 0;
	      userComb.msisdn = 0;
	      userComb.imsi = 0;
	      userComb.imei = 0;
	      if(! getUserCombFromObj(&userComb,tupleP->obj))
	      {
	        if( userComb.apnId != apnid)
		{
		  apnid = userComb.apnId;
		  queryApn( CDRBUILTINDB_DEFAULT_APNTABLE, apn, sizeof(apn), userComb.apnId);
		}
		if( userComb.flags.ipv4)
		{
		  inet_ntop(PF_INET, &userComb.ip, ipStr, sizeof( ipStr));
		}
		if( userComb.flags.ipv6)
		{
		  inet_ntop(PF_INET6, &userComb.ipv6, ipStr, sizeof( ipStr));
		}
		strftime(timeStr,sizeof(timeStr),"%Y-%m-%d %H:%M:%S",localtime_r(&userComb.time,&tM));
		snprintf(msgStr,sizeof(msgStr),"%s,%lu,%lu,%lu,%s,%s\r\n",timeStr,userComb.msisdn,userComb.imei,userComb.imsi,apn,ipStr);
		oamSendMsg(msgStr,strlen(msgStr),oamCmd->procNo);
	      }
	    }
	  }
	}
	else
	  retCode = res->errorCode;
	/*Clear res*/
	myDbFile_resClear( res);
      }
      else
	retCode = MYDBFILE_ERROR_ENOMEM;
      //printf("%s \n",tName);
      fromTm->tm_mon++;
      if( fromTm->tm_mon >= 12)
      {
        fromTm->tm_mon = 0;
	fromTm->tm_year++;
      }
      fromT = mktime(fromTm);
    }
    snprintf(msgStr,sizeof(msgStr),",,,,,\r\n");
    oamSendMsg( msgStr,strlen(msgStr), oamCmd->procNo);
    free( obj);
  }
  else
    retCode = MYDBFILE_ERROR_ENOMEM;
  return retCode;
}
