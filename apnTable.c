#include "string.h"
#include "stdint.h"
#include "tc-mydb.h"
#include "unistd.h"
#include "stdlib.h"
#include "pthread.h"
//struct _apnId {
//  uint8_t len;
//  uint16_t id;
//  char *apn;
//};
struct _charTable_8x16 {
  uint8_t len;
  uint16_t id;
  char *name;
};
int getCharTableFromObj_8x16( struct _charTable_8x16 *s, void *obj)
{
  int retCode=0;
  /*Copy len*/
  memcpy(&s->len,obj,sizeof(uint8_t));
  /*Copy id*/
  memcpy(&s->id,obj+sizeof(uint8_t),sizeof(uint16_t));
  /*Point to name*/
  s->name=obj+sizeof(uint8_t)+sizeof(uint16_t);
  return retCode;
}
int cmp_apnIdTable(void *pA, void *pB)
{
  int retCode=0;
  //char name[1024];
  struct _charTable_8x16 apnTA = {0};
  struct _charTable_8x16 apnTB = {0};
  getCharTableFromObj_8x16( &apnTA, pA);
  getCharTableFromObj_8x16( &apnTB, pB);
  if( apnTA.len > apnTB.len)
    retCode = 1;
  else if( apnTA.len < apnTB.len)
    retCode = -1;
  else
    retCode = strncmp(apnTA.name,apnTB.name,apnTA.len);
  //printf("cmp_apnIdTable ");
  //snprintf(name,apnTA.len,"%s",apnTA.name);
  //printf("apnA:%s, ",name);
  //snprintf(name,apnTB.len,"%s",apnTB.name);
  //printf("apnB:%s, ",name);
  //printf("cmp_apnIdTable idA:%u, idB:%u, retCode:%d\n",apnTA.id,apnTB.id,retCode);
  return retCode;
}

int getMaxApnId( char *name, uint16_t *id)
{
  int retCode=0;
  struct _myDbFile_res *res=NULL;
  struct _charTable_8x16 apnId={0};
  retCode = myDbFile_rewindTableIterator( name);
  if( !retCode)
  {
    while((res = myDbFile_getNextTableRow( name)))
    {
      if( (res->status == MYDBFILE_RES_TUPLESOK) && res->numTuples)
      {
	if( !getCharTableFromObj_8x16( &apnId, res->objs->obj))
	{
	  if( apnId.id > *id)
	    *id = apnId.id;
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
void *createCharTableObj_8x16( uint16_t id, char *name)
{
  void *obj=NULL;
  uint8_t len=0;
  len = sizeof(uint16_t)+strlen(name);
  obj = calloc(1,len+sizeof(uint8_t));
  if( obj)
  {
    /*Copy len*/
    memcpy(obj,&len,sizeof(uint8_t));
    /*Copy id*/
    memcpy(obj+sizeof(uint8_t),&id,sizeof(uint16_t));
    /*Copy name*/
    memcpy(obj+sizeof(uint8_t)+sizeof(uint16_t),name,len-sizeof(uint16_t));
  }
  return obj;
}
uint16_t getApnId( char *name, char *apn, uint16_t *serial, pthread_mutex_t *mutex)
{
  uint16_t id=0, idAux=0;
  struct _charTable_8x16 apnId={0};
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  void *obj=NULL;
  /*Lock*/
  pthread_mutex_lock(mutex);
  idAux = (*serial)+1;
  obj = createCharTableObj_8x16(idAux,apn);
  if( obj)
  {
    tuple.obj = obj;
    res = myDbFile_insert( name,&tuple);
    if( res)
    {
      /*New apn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        //getCharTableFromObj_8x16( &apnId, res->objs->obj);
        *serial = idAux;
	id = idAux;
        //printf("addApn %d\n",res->errorCode);
      }
      /*apn already exist, get id*/
      else if( (res->status==MYDBFILE_RES_FATALERROR) && (res->errorCode==MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION))
      {
        getCharTableFromObj_8x16(&apnId,res->objs->obj);
        //printf("numTuples:%u,MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION\n",res->numTuples);
	id = apnId.id;
	free( obj);
      }
      else
      {
        //printf("addApn %d\n",res->errorCode);
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    else
    {
        //printf("nomem \n");
      free( obj);
    }
  }
  /*Unlock*/
  pthread_mutex_unlock(mutex);
  return id;
}
int cmp_apnIdTableCust(void *pA, void *pB)
{
  int retCode=0;
  struct _charTable_8x16 apnTA = {0};
  struct _charTable_8x16 apnTB = {0};
  getCharTableFromObj_8x16( &apnTA, pA);
  getCharTableFromObj_8x16( &apnTB, pB);
  if( apnTA.id != apnTB.id)
    retCode = 1;
  return retCode;
}
int queryApn( char *tableName, char *apn, unsigned int len, uint32_t apnid)
{
  int retCode=0;
  struct _charTable_8x16 apnId={0};
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  void *obj=NULL;
  apn[0] = '\0';
  obj = createCharTableObj_8x16(apnid,apn);
  if( obj)
  {
    tuple.obj = obj;
    res = myDbFile_queryCust( tableName,&tuple,cmp_apnIdTableCust);
    if( res)
    {
      /*New apn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
	if( res->numTuples)
	{
	  getCharTableFromObj_8x16(&apnId,res->objs->obj);
          strncpy(apn,apnId.name,len);
        }
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    free( obj);
  }
  return retCode;
}
int cmp_apnIdTableCust2(void *pA, void *pB)
{
  int retCode=0;
  return retCode;
}
int queryApnTable( char *tableName, struct _oamCommand *oam)
{
  int retCode=0;
  struct _myDbFile_tuple tuple={0}, *tupleP=NULL;
  struct _myDbFile_res *res=NULL;
  struct _charTable_8x16 apnId={0};
  char apn[256];
  void *obj=NULL;
  snprintf(apn,sizeof(apn),"id,apn,\r\n");
  oamSendMsg( apn, strlen(apn), oam->procNo);
  apn[0] = '\0';
  obj = createCharTableObj_8x16(0,apn);
  if( obj)
  {
    tuple.obj = obj;
    res = myDbFile_queryCust( tableName,&tuple,cmp_apnIdTableCust2);
    if( res)
    {
      /*New apn insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
	for( tupleP=res->objs;tupleP;tupleP=tupleP->next)
	{
	  getCharTableFromObj_8x16(&apnId,tupleP->obj);
	  snprintf(apn,sizeof(apn),"%u,%s,\r\n",apnId.id,apnId.name);
	  oamSendMsg( apn, strlen(apn), oam->procNo);
	}
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    free( obj);
  }
  snprintf(apn,sizeof(apn),",,\r\n");
  oamSendMsg( apn, strlen(apn), oam->procNo);
  return retCode;
}
