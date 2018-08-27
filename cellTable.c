#include "stdint.h"
#include "tc-mydb.h"
#include "cdrDecoder.h"
#include "stdlib.h"
#include "unistd.h"
#include "pthread.h"
#include "string.h"

int cmp_cellIdTable( void *pA, void *pB)
{
  int retCode=0;
  struct _cellTable *cellA=pA;
  struct _cellTable *cellB=pB;
  /*Compare CELL*/
  if( cellA->ci > cellB->ci)
    retCode = 1;
  else if( cellA->ci < cellB->ci)
    retCode = -1;
  else
  {
    /*Compare LAC*/
    if( cellA->lac > cellB->lac)
      retCode = 1;
    else if( cellA->lac < cellB->lac)
      retCode = -1;
    else
    {
      /*Compare MNC*/
      if( cellA->mnc > cellB->mnc)
	retCode = 1;
      else if( cellA->mnc < cellB->mnc)
	retCode = -1;
      else
      {
        /*Compare MCC*/
	if( cellA->mcc > cellB->mcc)
	  retCode = 1;
	else if( cellA->mcc < cellB->mcc)
	  retCode = -1;
	else
	{
	  /*Compare RATT*/
	  if( cellA->ratt > cellB->ratt)
	    retCode = 1;
	  else if( cellA->ratt < cellB->ratt)
	    retCode = -1;
	}
      }
    }
  }
  return retCode;
}
int getMaxCellId( char *name, uint32_t *id)
{
  int retCode = 0;
  struct _myDbFile_res *res=NULL;
  struct _cellTable *cellTable=NULL;
  retCode = myDbFile_rewindTableIterator( name);
  if( !retCode)
  {
    while((res = myDbFile_getNextTableRow( name)))
    {
      if( (res->status == MYDBFILE_RES_TUPLESOK) && res->numTuples)
      {
        cellTable = res->objs->obj;
	//if( !getCharTableFromObj_8x16( &apnId, res->objs->obj))
	//{
	  if( cellTable->id > *id)
	    *id = cellTable->id;
	//}
	myDbFile_resClear( res);
      }
      else
      {
        retCode = res->errorCode;
	myDbFile_resClear( res);
        break;
      }
    }
  }
  return retCode;
}
uint32_t getCellId( char *name, struct _cellTable *cellStruct, uint32_t *serial, pthread_mutex_t *mutex)
{
  uint32_t id=0;
  struct _cellTable *cell=NULL;
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  /*Lock*/
  pthread_mutex_lock(mutex);
  cell = calloc(1,sizeof(struct _cellTable));
  if( cell)
  {
    *cell = *cellStruct;
    cell->id = (*serial)+1;
    tuple.obj = cell;
    res = myDbFile_insert( name,&tuple);
    if( res)
    {
      /*New cell insert*/
      if( res->status == MYDBFILE_RES_TUPLESOK)
      {
        *serial = cell->id;
	id = cell->id;
      }
      /*cell already exist, get id*/
      else if( (res->status==MYDBFILE_RES_FATALERROR) && (res->errorCode==MYDBFILE_ERROR_UNIQUECONSTRAINTVIOLATION))
      {
	free(cell);
	cell = res->objs->obj;
	id = cell->id;
      }
      /*Clear res*/
      myDbFile_resClear( res);
    }
    else
    {
      free( cell);
    }
  }
  /*Unlock*/
  pthread_mutex_unlock(mutex);
  return id;
}
int cmp_cellIdTableCust( void *pA, void *pB)
{
  int retCode=0;
  struct _cellTable *cellA=pA;
  struct _cellTable *cellB=pB;
  /*Compare CELL*/
  if( cellA->id != cellB->id)
    retCode = 1;
  return retCode;
}
int cmp_cellIdTableCust2( void *pA, void *pB)
{
  int retCode=0;
  return retCode;
}
int queryCell( char *tableName, struct _cellTable *cellTable)
{
  int retCode=0;
  struct _myDbFile_tuple tuple={0};
  struct _myDbFile_res *res=NULL;
  tuple.obj = cellTable;
  res = myDbFile_queryCust( tableName,&tuple,cmp_cellIdTableCust);
  if( res)
  {
    /*New cell insert*/
    if( res->status == MYDBFILE_RES_TUPLESOK)
    {
      if( res->numTuples)
	*cellTable = *( struct _cellTable *)res->objs->obj;
    }
    /*Clear res*/
    myDbFile_resClear( res);
  }
  return retCode;
}
int queryCellTable( char *tableName, struct _oamCommand *oam)
{
  int retCode=0;
  struct _myDbFile_tuple tuple={0}, *tupleP=NULL;
  struct _myDbFile_res *res=NULL;
  struct _cellTable cellTable={0};
  char cellStr[256];
  snprintf(cellStr,sizeof(cellStr),"id,ratt,mcc,mnc,lac/tac,ci/eci\r\n");
  oamSendMsg( cellStr, strlen(cellStr), oam->procNo);
  tuple.obj = &cellTable;
  res = myDbFile_queryCust( tableName,&tuple,cmp_cellIdTableCust2);
  if( res)
  {
    /*New cell insert*/
    if( res->status == MYDBFILE_RES_TUPLESOK)
    {
      for( tupleP=res->objs;tupleP;tupleP=tupleP->next)
      {
	cellTable = *( struct _cellTable *)tupleP->obj;
	snprintf(cellStr,sizeof(cellStr),"%u,%u,%u,%u,%u,%u\r\n",cellTable.id,cellTable.ratt,cellTable.mcc,cellTable.mnc,cellTable.lac,cellTable.ci);
	oamSendMsg( cellStr, strlen(cellStr), oam->procNo);
      }
    }
    /*Clear res*/
    myDbFile_resClear( res);
  }
  snprintf(cellStr,sizeof(cellStr),",,,,,\r\n");
  oamSendMsg( cellStr, strlen(cellStr), oam->procNo);
  return retCode;
}
