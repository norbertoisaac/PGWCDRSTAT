#include "cdrDecoder.h"
#include "stdio.h"
#include "time.h"
#include "tc-mydb.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include <unistd.h>
#include "pthread.h"
void *createNotifyInfoObj( struct _notifyInfo *notifyInfo)
{
  void *obj=NULL, *objAux=NULL;
  uint8_t len=0;
  uint8_t emailLen=0;
  unsigned char i=0;
  len = 1+ 8+ 8+ 1;
  for( i=0; i<notifyInfo->emailCount; i++)
  {
    len += 1+strlen(notifyInfo->email[i]);
  }
  obj = calloc(1,1+len);
  if( obj)
  {
    objAux = obj;
    /*Copy len*/
    memcpy(objAux,&len,1);
    objAux += 1;
    /*Copy flags*/
    memcpy(objAux,&notifyInfo->flags,1);
    objAux += 1;
    /*Copy msisdn*/
    memcpy(objAux,&notifyInfo->msisdn,8);
    objAux += 8;
    /*Copy imei*/
    memcpy(objAux,&notifyInfo->imei,8);
    objAux += 8;
    /*Copy email count*/
    memcpy(objAux,&notifyInfo->emailCount,1);
    objAux += 1;
    /*Copy email*/
    for( i=0; i<notifyInfo->emailCount; i++)
    {
      emailLen = strlen(notifyInfo->email[i]);
      memcpy(objAux,&emailLen,1);
      objAux += 1;
      memcpy(objAux,notifyInfo->email[i],emailLen);
      objAux += emailLen;
    }
  }
  return obj;
}
int addNotifySubscriber( struct _notifyInfo *notifyInfo)
{
  int retCode=0;
  struct _myDbFile_res *res=NULL;
  struct _myDbFile_tuple tuple={0};
  tuple.obj = createNotifyInfoObj( notifyInfo);
  if( tuple.obj)
  {
    res = myDbFile_insert( CDRBUILTINDB_DEFAULT_NOTIFYTABLE, &tuple);
    if( res)
    {
      if( res->status != MYDBFILE_RES_TUPLESOK)
      {
        retCode = -1;
        free( tuple.obj);
      }
      myDbFile_resClear( res);
    }
    //free( tuple.obj);
  }
  return retCode;
}
int getNotifyInfoFromObj( struct _notifyInfo *notifyInfo, void *obj)
{
  int retCode=0;
  uint8_t len=0;
  uint8_t emailLen=0;
  uint8_t i=0;
  /*Copy len*/
  memcpy(&len,obj,1);
  obj += 1;
  /*Copy flags*/
  memcpy(&notifyInfo->flags,obj,1);
  obj += 1;
  /*Copy msisdn*/
  memcpy(&notifyInfo->msisdn,obj,8);
  obj += 8;
  /*Copy imei*/
  memcpy(&notifyInfo->imei,obj,8);
  obj += 8;
  /*Copy email count*/
  memcpy(&notifyInfo->emailCount,obj,1);
  obj += 1;
  /*Copy email*/
  notifyInfo->email = calloc(notifyInfo->emailCount,sizeof(char *));
  for(i=0;i<notifyInfo->emailCount;i++)
  {
    memcpy(&emailLen,obj,1);
    obj += 1;
    notifyInfo->email[i] = calloc( 1,1+emailLen);
    memcpy(notifyInfo->email[i],obj,emailLen);
    obj += emailLen;
  }
  return retCode;
}
void freeNotifyInfo( struct _notifyInfo *notifyInfo)
{
  uint8_t i=0;
  for(i=0;i<notifyInfo->emailCount;i++)
  {
    free( notifyInfo->email[i]);
  }
  free( notifyInfo->email);
}
int cmp_notifyCombTable(void *pA, void *pB)
{
  int retCode=0;
  struct _notifyInfo notifyInfoA={{0}};
  struct _notifyInfo notifyInfoB={{0}};
  getNotifyInfoFromObj( &notifyInfoA, pA);
  getNotifyInfoFromObj( &notifyInfoB, pB);
  if( notifyInfoA.flags.msisdn)
  {
    if( notifyInfoA.msisdn > notifyInfoB.msisdn)
      retCode = 1;
    else if( notifyInfoA.msisdn < notifyInfoB.msisdn)
      retCode = -1;
  }
  else if(  notifyInfoA.flags.imei)
  {
    if( notifyInfoA.imei > notifyInfoB.imei)
      retCode = 1;
    else if( notifyInfoA.imei < notifyInfoB.imei)
      retCode = -1;
  }
  //printf ("cmp_subsIdTable idA:%u, msisdnA:%lu, idB:%u, msisdnB:%lu, retCode:%d 0x%p 0x%p\n",msisdnTA.id,msisdnTA.msisdn,msisdnTB.id,msisdnTB.msisdn,retCode,pA,pB);
  freeNotifyInfo( &notifyInfoA);
  freeNotifyInfo( &notifyInfoB);
  return retCode;
}
int getNotifyInfoMsisdn( struct _notifyInfo *notifyInfo)
{
  int retCode=1;
  struct _myDbFile_res *res=NULL;
  struct _myDbFile_tuple tuple={0};
  tuple.obj = createNotifyInfoObj( notifyInfo);
  if( tuple.obj)
  {
    res = myDbFile_query( CDRBUILTINDB_DEFAULT_NOTIFYTABLE, &tuple);
    if( res)
    {
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        if( res->numTuples)
	{
	  getNotifyInfoFromObj( notifyInfo, res->objs->obj);
	  retCode = 0;
	}
      }
      myDbFile_resClear( res);
    }
    free( tuple.obj);
  }
  return retCode;
}
int listNotifyInfo( struct _oamCommand *oamCmd)
{
  int retCode=0;
  struct _myDbFile_res *res=NULL;
  char msgStr[1024];
  struct _notifyInfo notifyInfo = {{0}};
  uint8_t i=0;
  uint16_t len=0;
  myDbFile_rewindTableIterator( CDRBUILTINDB_DEFAULT_NOTIFYTABLE);
  while( (res = myDbFile_getNextTableRow( CDRBUILTINDB_DEFAULT_NOTIFYTABLE)))
  {
    if( res->status == MYDBFILE_RES_TUPLESOK)
    {
      if( res->numTuples)
      {
        notifyInfo = (struct _notifyInfo){{0}};
	getNotifyInfoFromObj( &notifyInfo, res->objs->obj);
	len = 0;
	len = snprintf(msgStr,sizeof(msgStr),"%lu,",notifyInfo.msisdn);
	for(i=0;i<notifyInfo.emailCount;i++)
	{
	  len += snprintf(msgStr+len,sizeof(msgStr)-len,"%s,",notifyInfo.email[i]);
	}
	len += snprintf(msgStr+len,sizeof(msgStr)-len,"\n\r");
	oamSendMsg(msgStr, len, oamCmd->procNo);
	freeNotifyInfo( &notifyInfo);
	retCode = 0;
	myDbFile_resClear( res);
      }
      else
      {
	myDbFile_resClear( res);
	break;
      }
    }
    else
    {
      myDbFile_resClear( res);
      break;
    }
  }
  return retCode;
}
int delNotifySubscriber( struct _notifyInfo *notifyInfo)
{
  int retCode=0;
  struct _myDbFile_res *res=NULL;
  struct _myDbFile_tuple tuple={0};
  tuple.obj = createNotifyInfoObj( notifyInfo);
  if( tuple.obj)
  {
    res = myDbFile_delete( CDRBUILTINDB_DEFAULT_NOTIFYTABLE, &tuple);
    if( res)
    {
      if( res->status != MYDBFILE_RES_TUPLESOK)
      {
        retCode = -1;
        free( tuple.obj);
      }
      myDbFile_resClear( res);
    }
    //free( tuple.obj);
  }
  return retCode;
}
//uint32_t getSubsId( char *name, uint64_t msisdn, uint32_t *serial, unsigned char *lock)
//{
//  uint32_t id=0;
//  struct _subsId subsId={0};
//  struct _subsIdTable *subsIdTable=NULL;
//  struct _myDbFile_tuple tuple={0};
//  struct _myDbFile_res *res=NULL;
//  /*Lock*/
//  pthread_mutex_lock(&msisdn_mut);
//  //while (*lock) usleep(1000);
//  //*lock = 1;
//  subsId.id = ++*serial;
//  subsId.msisdn = msisdn;
//  subsIdTable = createMsisdnTableObj(&subsId);
//  if( subsIdTable)
//  {
//    tuple.obj = subsIdTable;
//    //printf("getSubsId tableId %d \n",tableId);
//    res = myDbFile_insert( name,&tuple);
//    if( res)
//    {
//      /*New msisdn insert*/
//      if( res->status == MYDBFILE_RES_TUPLESOK)
//      {
//        id = subsId.id;
//	//printf("MSISDN MYDBFILE_RES_TUPLESOK\n");
//      }
//      /*MSISDN already exist, get id*/
//      else if( (res->status==MYDBFILE_RES_FATALERROR) && (res->errorCode==MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION))
//      {
//        getSubsIdFromObj(&subsId,res->objs->obj);
//        //printf("numTuples:%u,MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION,id:%u\n",res->numTuples,subsId.id);
//	id = subsId.id;
//	free( subsIdTable);
//	(*serial)--;
//      }
//      else
//      {
//        //printf(" getSubsId error %d\n",res->errorCode);
//      }
//      /*Clear res*/
//      myDbFile_resClear( res);
//    }
//    else
//    {
//      //printf(" getSubsId nomem\n");
//      free( subsIdTable);
//      (*serial)--;
//    }
//  }
//  else
//    (*serial)--;
//  /*Unlock*/
//  //*lock = 0;
//  pthread_mutex_unlock(&msisdn_mut);
//  return id;
//}
//uint32_t querySubsId( char *tableName, uint64_t msisdn, pthread_mutex_t *mutex)
//{
//  uint32_t id=0;
//  struct _subsId subsId={0};
//  struct _subsIdTable *subsIdTable=NULL;
//  struct _myDbFile_tuple tuple={0};
//  struct _myDbFile_res *res=NULL;
//
//  subsId.id = 0;
//  subsId.msisdn = msisdn;
//  subsIdTable = createMsisdnTableObj(&subsId);
//  if( subsIdTable)
//  {
//    tuple.obj = subsIdTable;
//    res = myDbFile_query( tableName,&tuple);
//    //printf("querySubsId \n");
//    if( res)
//    {
//      /*New msisdn insert*/
//      if( res->status == MYDBFILE_RES_TUPLESOK)
//      {
//        if( res->numTuples)
//	{
//	  getSubsIdFromObj(&subsId,res->objs->obj);
//	  id = subsId.id;
//	}
//      }
//      /*Clear res*/
//      myDbFile_resClear( res);
//    }
//    free( subsIdTable);
//  }
//  return id;
//}
//uint64_t querySubsMsisdn( char *tableName, uint32_t id, pthread_mutex_t *mutex)
//{
//  uint64_t isdn=0;
//  struct _subsId subsId={0};
//  struct _subsIdTable *subsIdTable=NULL;
//  struct _myDbFile_tuple tuple={0};
//  struct _myDbFile_res *res=NULL;
//
//  subsId.id = id;
//  subsId.msisdn = 0;
//  subsIdTable = createMsisdnTableObj(&subsId);
//  if( subsIdTable)
//  {
//    tuple.obj = subsIdTable;
//    res = myDbFile_queryCust( tableName,&tuple,cmpSubsById);
//    //printf("querySubsMsisdn \n");
//    if( res)
//    {
//      /*New msisdn insert*/
//      if( res->status == MYDBFILE_RES_TUPLESOK)
//      {
//        if( res->numTuples)
//	{
//	  getSubsIdFromObj(&subsId,res->objs->obj);
//	  isdn = subsId.msisdn;
//	}
//      }
//      /*Clear res*/
//      myDbFile_resClear( res);
//    }
//    free( subsIdTable);
//  }
//  return isdn;
//}
