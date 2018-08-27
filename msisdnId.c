#include "stdio.h"
#include "time.h"
#include "tc-mydb.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include <unistd.h>
#include "pthread.h"
struct _subsIdTable {
  uint8_t len;
  unsigned idLen:3;
  unsigned msisdnLen:4;
  unsigned :1;
  uint8_t obj[11];
};
struct _subsId{
  uint32_t id;
  uint64_t msisdn;
};
int getSubsIdFromObj( struct _subsId *subsId, void *obj)
{
  int retCode=0;
  struct _subsIdTable *msisdnTA=obj;
  subsId->id = 0;
  memcpy(&subsId->id,msisdnTA->obj,msisdnTA->idLen);
  subsId->msisdn = 0;
  memcpy(&subsId->msisdn,msisdnTA->obj+msisdnTA->idLen,msisdnTA->msisdnLen);
  return retCode;
}
int cmp_subsIdTable(void *pA, void *pB)
{
  int retCode=0;
  struct _subsId msisdnTA={0};
  struct _subsId msisdnTB={0};
  getSubsIdFromObj( &msisdnTA, pA);
  getSubsIdFromObj( &msisdnTB, pB);
  if( msisdnTA.msisdn > msisdnTB.msisdn)
    retCode = 1;
  else if( msisdnTA.msisdn < msisdnTB.msisdn)
    retCode = -1;
  //printf ("cmp_subsIdTable idA:%u, msisdnA:%lu, idB:%u, msisdnB:%lu, retCode:%d 0x%p 0x%p\n",msisdnTA.id,msisdnTA.msisdn,msisdnTB.id,msisdnTB.msisdn,retCode,pA,pB);
  return retCode;
}
int cmpSubsById(void *pA, void *pB)
{
  int retCode=0;
  struct _subsId msisdnTA={0};
  struct _subsId msisdnTB={0};
  getSubsIdFromObj( &msisdnTA, pA);
  getSubsIdFromObj( &msisdnTB, pB);
  if( msisdnTA.id != msisdnTB.id)
    retCode = 1;
  //printf ("%u,%lu,%u,%lu,%d\n",msisdnTA.id,msisdnTA.msisdn,msisdnTB.id,msisdnTB.msisdn,retCode);
  return retCode;
}
struct _subsIdTable *createMsisdnTableObj(struct _subsId *subsId)
{
  struct _subsIdTable *subsIdTable=NULL;
  subsIdTable = calloc(1,sizeof(struct _subsIdTable));
  if( subsIdTable)
  {
    subsIdTable->idLen = numBytesOcuped( subsId->id);
    subsIdTable->msisdnLen = numBytesOcuped( subsId->msisdn);
    subsIdTable->len = subsIdTable->idLen+subsIdTable->msisdnLen+1;
    memcpy(subsIdTable->obj,&subsId->id,subsIdTable->idLen);
    memcpy(subsIdTable->obj+subsIdTable->idLen,&subsId->msisdn,subsIdTable->msisdnLen);
  }
  return subsIdTable;
}
int getMaxSubsId( char *name, uint32_t *id)
{
  int retCode=0;
  struct _myDbFile_res *res=NULL;
  struct _subsId subsId={0};
  retCode = myDbFile_rewindTableIterator( name);
  if( !retCode)
  {
    while((res = myDbFile_getNextTableRow(name)))
    {
      if( (res->status == MYDBFILE_RES_TUPLESOK) && res->numTuples)
      {
	if( !getSubsIdFromObj( &subsId, res->objs->obj))
	{
	  if( subsId.id > *id)
	    *id = subsId.id;
	}
	myDbFile_resClear( res);
      }
      else
      {
	myDbFile_resClear( res);
        break;
      }
    }
  }
  return retCode;
}
pthread_mutex_t msisdn_mut;
uint32_t getSubsId( char *name, uint64_t msisdn, uint32_t *serial, unsigned char *lock)
{
  uint32_t id=0;
  struct _subsId subsId={0};
  struct _subsIdTable *subsIdTable=NULL;
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  /*Lock*/
  pthread_mutex_lock(&msisdn_mut);
  //while (*lock) usleep(1000);
  //*lock = 1;
  subsId.id = ++*serial;
  subsId.msisdn = msisdn;
  subsIdTable = createMsisdnTableObj(&subsId);
  if( subsIdTable)
  {
    tuple.obj = subsIdTable;
    //printf("getSubsId tableId %d \n",tableId);
    res = myDbFile_insert( name,&tuple);
    if( res)
    {
      /*New msisdn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        id = subsId.id;
	//printf("MSISDN MYDBFILE_RES_TUPLESOK\n");
      }
      /*MSISDN already exist, get id*/
      else if( (res->status==MYDBFILE_RES_FATALERROR) && (res->errorCode==MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION))
      {
        getSubsIdFromObj(&subsId,res->objs->obj);
        //printf("numTuples:%u,MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION,id:%u\n",res->numTuples,subsId.id);
	id = subsId.id;
	free( subsIdTable);
	(*serial)--;
      }
      else
      {
        //printf(" getSubsId error %d\n",res->errorCode);
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    else
    {
      //printf(" getSubsId nomem\n");
      free( subsIdTable);
      (*serial)--;
    }
  }
  else
    (*serial)--;
  /*Unlock*/
  //*lock = 0;
  pthread_mutex_unlock(&msisdn_mut);
  return id;
}
uint32_t querySubsId( char *tableName, uint64_t msisdn, pthread_mutex_t *mutex)
{
  uint32_t id=0;
  struct _subsId subsId={0};
  struct _subsIdTable *subsIdTable=NULL;
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;

  subsId.id = 0;
  subsId.msisdn = msisdn;
  subsIdTable = createMsisdnTableObj(&subsId);
  if( subsIdTable)
  {
    tuple.obj = subsIdTable;
    res = myDbFile_query( tableName,&tuple);
    //printf("querySubsId \n");
    if( res)
    {
      /*New msisdn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        if( res->numTuples)
	{
	  getSubsIdFromObj(&subsId,res->objs->obj);
	  id = subsId.id;
	}
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    free( subsIdTable);
  }
  return id;
}
uint64_t querySubsMsisdn( char *tableName, uint32_t id, pthread_mutex_t *mutex)
{
  uint64_t isdn=0;
  struct _subsId subsId={0};
  struct _subsIdTable *subsIdTable=NULL;
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;

  subsId.id = id;
  subsId.msisdn = 0;
  subsIdTable = createMsisdnTableObj(&subsId);
  if( subsIdTable)
  {
    tuple.obj = subsIdTable;
    res = myDbFile_queryCust( tableName,&tuple,cmpSubsById);
    //printf("querySubsMsisdn \n");
    if( res)
    {
      /*New msisdn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        if( res->numTuples)
	{
	  getSubsIdFromObj(&subsId,res->objs->obj);
	  isdn = subsId.msisdn;
	}
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    free( subsIdTable);
  }
  return isdn;
}
