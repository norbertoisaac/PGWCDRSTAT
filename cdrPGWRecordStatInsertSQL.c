#include "cdrDecoder.h"
#include "stdio.h"
#include "time.h"
#include "tc-mydb.h"
#include "tc-mydebug.h"
#include <string.h>
#include "unistd.h"
#include <stdlib.h>
#include "tc-misc.h"
#include <arpa/inet.h>
extern unsigned int statTimeDiv;
int sendCopySQL(struct _pgCon *pgCon, char *sql, char *buffer, unsigned int buffer_count)
{
  int retCode=0;
  char msgStr[512];
  char logStr[512];
  PGresult *res;
  res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  if(PQresultStatus(res) == PGRES_COPY_IN)
  {
    if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    else
    {
      if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyEnd Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	retCode = -1;
      }
    }
  }
  else
  {
    snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  PQclear(res);
  res = PQgetResult(pgCon->pgCon);
  if( PQresultStatus(res)==PGRES_FATAL_ERROR)
  {
    snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  PQclear(res);
  return retCode;
}
/*Subscriber tracking insert data*/
int copyCdrSubsTrackTables(struct _pgCon *pgCon, void *it_substrkDB, void *substrkTableNameIt)
{
  #define buffer_1MB 1024*1024
  uint32_t buffer_count=0;
  uint32_t buffer_len=0;
  char *buffer=NULL;
  int retCode=0;
  char sql[5120];
  char msgStr[512];
  char logStr[512];
  struct _substrkSql *A=NULL;
  struct _substrkTableName *tableName=NULL;
  char timeStr[64];
  time_t t0;
  myDB_rewindIterator( substrkTableNameIt);
  while((  tableName = myDB_getNextRow(substrkTableNameIt)))
  {
    buffer_count = 0;
    /*
    snprintf(sql,sizeof(sql),\
      "COPY %s(tor,isdn,apnni,rattype,mcc,mnc,lac,ci,start,stop,imei,uplink,downlink)\
       FROM STDIN WITH (FORMAT csv);",\
       tableName->tableName);
    */
    snprintf(sql,sizeof(sql),\
      "COPY %s(tor,isdn,apnni,rattype,mcc,mnc,lac,ci)\
       FROM STDIN WITH (FORMAT csv);",\
       tableName->tableName);
    myDB_rewindIterator( it_substrkDB);
    while((A = myDB_getNextRow(it_substrkDB)))
    {
      t0 = mktime(&A->tor);
      if((t0>=mktime(&tableName->minTor))&&(t0<=mktime(&tableName->maxTor)))
      {
	if((buffer_len-buffer_count)<buffer_1MB)
	{
	  if(!buffer)
	  {
	    buffer = calloc(1,buffer_1MB);
	    buffer_len = buffer_1MB;
	  }
	  else
	  {
	    buffer = realloc(buffer,buffer_len+buffer_1MB);
	    buffer_len += buffer_1MB;
	  }
	}
	strftime(timeStr,sizeof(timeStr),"%Y-%m-%d %H:%M:%S",&(A->tor));
	//buffer_count += snprintf(buffer+buffer_count,buffer_len-buffer_count,
	//  "'%s',%lu,%s,%u,%u,%u,%u,%u,%s,%s,%lu,%lu,%lu\n",
	//  timeStr,
	//  A->isdn,
	//  A->apnni,
	//  A->rattype,
	//  A->mcc,
	//  A->mnc,
	//  A->lac,
	//  A->ci,
	//  A->start? "t":"f",
	//  A->stop? "t":"f",
	//  A->imei,
	//  A->uplink,
	//  A->downlink
	//  );
	buffer_count += snprintf(buffer+buffer_count,buffer_len-buffer_count,
	  "'%s',%lu,%s,%u,%u,%u,%u,%u\n",
	  timeStr,
	  A->isdn,
	  A->apnni,
	  A->rattype,
	  A->mcc,
	  A->mnc,
	  A->lac,
	  A->ci
	  );
      }
    }
    retCode = sendCopySQL(pgCon,sql,buffer,buffer_count);
    if(!retCode)
    {
      snprintf(sql,sizeof(sql),"UPDATE cdr.tables SET status=1 WHERE name='%s'",tableName->tableName);
      retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr));
      if(retCode)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,%s",__FUNCTION__,__LINE__,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	break;
      }
    }
    else
      break;
  }
  if(buffer) free(buffer);
  return retCode;
}
int copyCdrSubsTrack(struct _pgCon *pgCon, char *tableName, char *buffer, uint32_t buffer_count, void *substrkTableNameIt)
{
  int retCode=0;
  char sql[5120];
  char msgStr[512];
  char logStr[512];
  PGresult *res;
  //retCode = myDB_PQexec( pgCon, "BEGIN", msgStr, sizeof(msgStr));
  //if(!retCode)
  //{
    //retCode = subsTrackSqlTmpTable(pgCon,tableName);
    //if(!retCode)
    //{
      snprintf(sql,sizeof(sql),\
	"COPY %s(tor,isdn,apnni,rattype,mcc,mnc,lac,ci)\
	 FROM STDIN WITH (FORMAT csv);",\
	 tableName);
      res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
      if(PQresultStatus(res) == PGRES_COPY_IN)
      {
	if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  retCode = -1;
	}
	else
	{
	  if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	  {
	    snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyEnd Error",__FUNCTION__,__LINE__);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    retCode = -1;
	  }
	}
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	retCode = -1;
      }
      PQclear(res);
      res = PQgetResult(pgCon->pgCon);
      if( PQresultStatus(res)==PGRES_FATAL_ERROR)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	retCode = -1;
      }
      else
      {
        snprintf(sql,sizeof(sql),"UPDATE cdr.tables SET status=1 WHERE name='%s'",tableName);
	retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr));
	if(retCode)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,%s",__FUNCTION__,__LINE__,msgStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
      PQclear(res);
    //}
  //}
  //else
  //{
  //  snprintf(logStr,sizeof(logStr),"%s,%d,%s",__FUNCTION__,__LINE__,msgStr);
  //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //  retCode = -1;
  //}
  //myDB_PQexec( pgCon, "COMMIT", NULL, 0);
  //if(!retCode)
  //{
  //      snprintf(logStr,sizeof(logStr),"pn=%u, tracking insert start",pgCon->procId);
  //      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //  while((tableNameTrk = myDB_getNextRow(substrkTableNameIt)))
  //  {
  //    snprintf(sql,sizeof(sql),"INSERT INTO %s(tor,isdn,apnid,cellid) SELECT tor,msisdn,cdr.apntrk.id,cdr.celltrk.id FROM %s_substrk JOIN cdr.apntrk ON name=apnni JOIN cdr.celltrk ON cdr.celltrk.rattype=%s_substrk.rattype AND cdr.celltrk.mcc=%s_substrk.mcc AND cdr.celltrk.mnc=%s_substrk.mnc AND cdr.celltrk.lac=%s_substrk.lac AND cdr.celltrk.ci=%s_substrk.ci AND %s_substrk.tablename='%s';",tableNameTrk,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableNameTrk);
  //    //snprintf(sql,sizeof(sql),"INSERT INTO %s(tor,isdn,apnid,cellid) SELECT tor,msisdn,cdr.apntrk.id,cdr.ci.id FROM %s_substrk JOIN cdr.apntrk ON name=apnni JOIN cdr.ci ON cdr.ci.rattype=%s_substrk.rattype AND cdr.ci.mcc=%s_substrk.mcc AND cdr.ci.mnc=%s_substrk.mnc AND cdr.ci.lac=%s_substrk.lac AND cdr.ci.ci=%s_substrk.ci AND %s_substrk.tablename='%s';",tableNameTrk,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableNameTrk);
  //      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
  //    retCode = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
  //    if( retCode)
  //    {
  //      retCode = -1;
  //      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,retCode,msgStr);
  //      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  //    }
  //  }
  //}
  return retCode;
}
int insertCdrSubsTrack(struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  snprintf(sql,sizeof(sql),"INSERT INTO cdr.substrk(tor,isdn,apnid,cellid) SELECT tor,msisdn,cdr.apntrk.id,cdr.celltrk.id FROM %s_substrk JOIN cdr.apntrk ON name=apnni JOIN cdr.celltrk ON cdr.celltrk.rattype=%s_substrk.rattype AND cdr.celltrk.mcc=%s_substrk.mcc AND cdr.celltrk.mnc=%s_substrk.mnc AND cdr.celltrk.lac=%s_substrk.lac AND cdr.celltrk.ci=%s_substrk.ci;",tableName,tableName,tableName,tableName,tableName,tableName);
  retCode = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
  if( retCode)
  {
    retCode = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,retCode,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return retCode;
}
/*Subscriber tracking temporary table*/
//int subsTrackSqlTmpTable(struct _pgCon *pgCon, char *tableName, struct tm minTor, struct tm maxTor)
int subsTrackSqlTmpTable(struct _pgCon *pgCon, void *substrkTableNameIt)
{
  int retCode=0;
  int retInt=0;
  int i=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  char minTorStr[64];
  char maxTorStr[64];
  PGresult *res=NULL;
  struct _substrkTableName *substrkTableName=NULL;
  myDB_rewindIterator( substrkTableNameIt);
  while((substrkTableName = myDB_getNextRow(substrkTableNameIt))&&!retCode)
  {
    strftime(minTorStr,sizeof(minTorStr),"%Y-%m-%d %H:%M:%S",&substrkTableName->minTor);
    strftime(maxTorStr,sizeof(maxTorStr),"%Y-%m-%d %H:%M:%S",&substrkTableName->maxTor);
    snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT '%s'::varchar AS name,'%s'::timestamp without time zone AS mintor,'%s'::timestamp without time zone AS maxtor,'substrk' AS ttype), upd1 AS (UPDATE cdr.tables SET ctime=now() FROM sel1 WHERE cdr.tables.name=sel1.name) INSERT INTO cdr.tables(name,mintor,maxtor,ttype) SELECT name,mintor,maxtor,ttype FROM sel1 WHERE name NOT IN (SELECT name FROM cdr.tables) RETURNING cdr.tables.name,cdr.tables.mintor,cdr.tables.maxtor",substrkTableName->tableName,minTorStr,maxTorStr);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if((PQresultStatus(res)==PGRES_SINGLE_TUPLE)||(PQresultStatus(res)==PGRES_TUPLES_OK))
    {
      retInt = PQntuples(res);
      for(i=0;i<retInt;i++)
      {
	snprintf(sql,sizeof(sql),"create table IF NOT EXISTS %s( CHECK(tor>='%s' AND tor<='%s')) INHERITS (cdr.substrk); CREATE INDEX ON %s(tor,isdn);",substrkTableName->tableName,minTorStr,maxTorStr,substrkTableName->tableName);
	retCode = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
	if( retCode)
	{
	  snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,retCode,msgStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  break;
	}
      }
    }
    else
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,retCode,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
    PQclear(res);
  }
  return retCode;
}
int addLosdTables(struct _pgCon *pgCon, void *it_losdTablesDB)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  struct _losdTables *losdTables;
  char timestamp[64];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  while( (losdTables = myDB_getNextRow( it_losdTablesDB)))
  {
    strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:00",&losdTables->tm);
    snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT 'cdr_losd_60_%s'::varchar AS name,60::int AS gran,'%s'::timestamp without time zone AS ch,'losd' AS ttype), upd1 AS (UPDATE cdr_tables SET ctime=now() FROM sel1 WHERE cdr_tables.name=sel1.name) INSERT INTO cdr_tables(name,gran,tst,ttype) SELECT name,gran,ch,ttype FROM sel1 WHERE name NOT IN (SELECT name FROM cdr_tables) RETURNING cdr_tables.name,cdr_tables.tst;",losdTables->name,timestamp);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
      break;
    }
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"CREATE TABLE IF NOT EXISTS %s (fecha timestamp without time zone DEFAULT '%s' CHECK (fecha=timestamp '%s')) INHERITS (cdr_losd);",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,1));
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r<0)
      {
	r = -1;
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	break;
      }
      else
      {
	snprintf(sql,sizeof(sql),"CREATE INDEX i_%s ON %s(fecha,pgwcdrcmbid,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue);", PQgetvalue(res,i,0), PQgetvalue(res,i,0));
	myDB_PQexec( pgCon, sql, NULL,0);
      }
    }
    PQclear(res);
  }
  return r;
}
int addLosdTableName(struct _pgCon *pgCon, char *tableName, char *tst, int gran)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT '%s'::varchar AS name,%d::int AS gran,'%s'::timestamp without time zone AS ch,'losd' AS ttype), upd1 AS (UPDATE cdr_tables SET ctime=now(),status=0 FROM sel1 WHERE cdr_tables.name=sel1.name) INSERT INTO cdr_tables(name,gran,tst,ttype) SELECT name,gran,ch,ttype FROM sel1 WHERE name NOT IN (SELECT name FROM cdr_tables) RETURNING cdr_tables.name,cdr_tables.tst,cdr_tables.gran,cdr_tables.tst+interval '%d sec';",tableName,gran,tst,gran);
  res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  if((PQresultStatus(res)==PGRES_TUPLES_OK)||(PQresultStatus(res)==PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"CREATE TABLE IF NOT EXISTS %s (fecha timestamp without time zone DEFAULT '%s' CHECK (fecha>=(timestamp '%s') AND fecha<('%s'::timestamp)), CHECK (gran=%s)) INHERITS (cdr_losd);",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,1),PQgetvalue(res,i,3),PQgetvalue(res,i,2));
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	break;
      }
      else
      {
	snprintf(sql,sizeof(sql),"\
	CREATE INDEX i1_%s ON %s(fecha,apnni,cc,crbn);\
	CREATE INDEX i3_%s ON %s(gwaddr,snaddr,sntype,rattype);\
	CREATE INDEX i4_%s ON %s(ratinggroup,serviceidentifier);\
	CREATE INDEX i5_%s ON %s(resultcode,failurehandlingcontinue);\
	  ",PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0));
	myDB_PQexec( pgCon, sql, NULL,0);
	snprintf(logStr,sizeof(logStr),"CREATE TABLE %s",PQgetvalue(res,i,0));
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  else
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  PQclear(res);
  return r;
}
int addLotvTableMeanName(struct _pgCon *pgCon, char *tableName, int tableNameLen, int dow, int gran)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  snprintf(tableName, tableNameLen,"cdr_lotv_m_%d_%d",gran,dow);
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT '%s'::varchar AS name,%d::int AS dow,'lotv_m' AS ttype, %d::int AS gran),\
    upd1 AS (UPDATE cdr_tables SET ctime=now(),status=0 FROM sel1 WHERE cdr_tables.name=sel1.name)\
    INSERT INTO cdr_tables(name,gran,dow,ttype)\
      SELECT name,gran,dow,ttype\
      FROM sel1\
      WHERE name NOT IN (SELECT name FROM cdr_tables) RETURNING cdr_tables.name,cdr_tables.dow,cdr_tables.gran;\
    ",tableName,dow,gran);
  res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  if((PQresultStatus(res)==PGRES_TUPLES_OK)||(PQresultStatus(res)==PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"\
        CREATE TABLE %s (id bigserial PRIMARY KEY, lastupdate timestamp DEFAULT now(), dow int DEFAULT %s, gran int DEFAULT %s CHECK (dow=%s)) INHERITS (cdr_lotv_m);\
	CREATE INDEX i_%s ON %s(tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic)\
      ;",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,2),PQgetvalue(res,i,1),PQgetvalue(res,i,0),PQgetvalue(res,i,0));
      r = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr));
      if( !r)
      {
        snprintf(logStr,sizeof(logStr),"CREATE TABLE %s",PQgetvalue(res,i,0));
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,msgStr);
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
        break;
      }
    }
  }
  else
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  PQclear(res);
  return r;
}
int addLotvTableName(struct _pgCon *pgCon, char *tableName, char *tst, int gran, unsigned int interval)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT '%s'::varchar AS name,%d::int AS gran,'%s'::timestamp without time zone AS ch,'lotv' AS ttype), upd1 AS (UPDATE cdr_tables SET ctime=now(),status=0 FROM sel1 WHERE cdr_tables.name=sel1.name) INSERT INTO cdr_tables(name,gran,tst,ttype) SELECT name,gran,ch,ttype FROM sel1 WHERE name NOT IN (SELECT name FROM cdr_tables) RETURNING cdr_tables.name,cdr_tables.tst,cdr_tables.gran,cdr_tables.tst+interval '%d sec';",tableName,gran,tst,interval);
  res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  if((PQresultStatus(res)==PGRES_TUPLES_OK)||(PQresultStatus(res)==PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"CREATE TABLE IF NOT EXISTS %s (id bigserial PRIMARY KEY, fecha timestamp without time zone DEFAULT '%s' CHECK (fecha>=(timestamp '%s') AND fecha<('%s'::timestamp)), CHECK (gran=%s)) INHERITS (cdr_lotv);",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,1),PQgetvalue(res,i,3),PQgetvalue(res,i,2));
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	break;
      }
      else
      {
	snprintf(sql,sizeof(sql),"CREATE INDEX i_apnni_%s ON %s(fecha,gran,apnni); CREATE INDEX i_ci_%s ON %s(rattype,mcc,mnc,lac,ci); CREATE INDEX i_sngwn_%s ON %s(gwaddr,snaddr);",PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0));
	myDB_PQexec( pgCon, sql, NULL,0);
	snprintf(logStr,sizeof(logStr),"CREATE TABLE %s",PQgetvalue(res,i,0));
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  else
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  PQclear(res);
  return r;
}
int addLotvTables(struct _pgCon *pgCon, void *it_lotvTablesDB)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  struct _lotvTables *lotvTables=NULL;
  char timestamp[32];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  while( (lotvTables = myDB_getNextRow( it_lotvTablesDB)))
  {
    strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:00",&lotvTables->tm);
    snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT 'cdr_lotv_60_%s'::varchar AS name,60::int AS gran,'%s'::timestamp without time zone AS ch,'lotv' AS ttype), upd1 AS (UPDATE cdr_tables SET ctime=now() FROM sel1 WHERE cdr_tables.name=sel1.name) INSERT INTO cdr_tables(name,gran,tst,ttype) SELECT name,gran,ch,ttype FROM sel1 WHERE name NOT IN (SELECT name FROM cdr_tables) RETURNING cdr_tables.name,cdr_tables.tst;",lotvTables->name,timestamp);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
      break;
    }
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"CREATE TABLE IF NOT EXISTS %s (fecha timestamp without time zone DEFAULT '%s' CHECK (fecha=timestamp '%s')) INHERITS (cdr_lotv);",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,1));
      //snprintf(sql,sizeof(sql),"CREATE TABLE IF NOT EXISTS cdr_lotv_60_%s (fecha timestamp without time zone DEFAULT '%.4s-%.2s-%.2s %.2s:%.2s:00',pgwcdrcmbid bigint REFERENCES cdr_pgwcdrcmb(id) ON DELETE RESTRICT CHECK (fecha=timestamp '%.4s-%.2s-%.2s %.2s:%.2s:00')) INHERITS (cdr_lotv);",lotvTableTime,lotvTableTime,lotvTableTime+4,lotvTableTime+6,lotvTableTime+8,lotvTableTime+10,lotvTableTime,lotvTableTime+4,lotvTableTime+6,lotvTableTime+8,lotvTableTime+10);
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r<0)
      {
	r = -1;
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	break;
      }
      else
      {
	snprintf(sql,sizeof(sql),"CREATE INDEX i_%s ON %s(pgwcdrcmbid,withtraffic);",PQgetvalue(res,i,0),PQgetvalue(res,i,0));
	myDB_PQexec( pgCon, sql, NULL,0);
      }
    }
    PQclear(res);
  }
  return r;
}
int addPgwCdrCmb(struct _pgCon *pgCon, void *it, char *tableName)
{
  #define PGWCDRCMB_LEN 11
  #define PGWCDRCMB_BUFFER_LEN 1024*1024
  char *buffer=NULL;
  uint32_t buffer_count=0, buffer_len=PGWCDRCMB_BUFFER_LEN;
  int r=0;
  struct _cdr_pgwcdrcmb *cdr_pgwcdrcmb=NULL;
  char sql[10512];
  char msgStr[10512];
  char logStr[10512];
  char timestamp[32];
  //char *values[PGWCDRCMB_LEN]={NULL};
  //int len[PGWCDRCMB_LEN]={0};
  //int format[PGWCDRCMB_LEN]={0};
  //uint64_t id;
  //uint32_t cdrCi_id, cc, gwaddress_id, snaddress_id, apnId_id;
  //uint16_t apnSelectionMode, chChSelectionMode;
  //uint8_t dynamicAddressFlag, iMSsignalingContext;
  char preparedName[64];
  PGresult *res;
  snprintf(preparedName,sizeof(preparedName),"%s_pgwcdrcmb",tableName);
  buffer = malloc(buffer_len);
  if(buffer==NULL)
    return -1;
  while( (cdr_pgwcdrcmb = myDB_getNextRow( it)))
  {
    if((buffer_len-buffer_count)<1024)
    {
      buffer = realloc(buffer,buffer_len+PGWCDRCMB_BUFFER_LEN);
      if(buffer)
      {
        buffer_len+=PGWCDRCMB_BUFFER_LEN;
      }
      else
        return -1;
    }
    strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&cdr_pgwcdrcmb->tst_tm);
    buffer_count+=snprintf(buffer+buffer_count,buffer_len-buffer_count,\
      "%lu,'%s',%u,%u,%u,%s,%u,%u,%u,%s,%u\n",\
      //id bigint primary key
      cdr_pgwcdrcmb->id,\
      //lastupdate TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
      timestamp,\
      //pgwid int
      cdr_pgwcdrcmb->gwaddress_id,\
      //sgwid int
      cdr_pgwcdrcmb->snaddress_id,\
      //apnid int
      cdr_pgwcdrcmb->apnId_id,\
      //dynamicAddressFlag boolean default true
      cdr_pgwcdrcmb->dynamicAddressFlag ? "t":"f",\
      //apnSelectionMode smallint
      cdr_pgwcdrcmb->apnSelectionMode,\
      //cc int NOT NULL
      cdr_pgwcdrcmb->cc,\
      //chChSelectionMode smallint
      cdr_pgwcdrcmb->chChSelectionMode,\
      //iMSsignalingContext boolean default false
      cdr_pgwcdrcmb->iMSsignalingContext ? "t":"f",\
      //cid int
      cdr_pgwcdrcmb->cdrCi_id
      );
    continue;
    //INSERT INTO %s(id,lastupdate,pgwid,sgwid,apnid,dynamicAddressFlag,apnSelectionMode,cc,chChSelectionMode,iMSsignalingContext,cid) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    /*id*/
    //id = htobe64(cdr_pgwcdrcmb->id);
    //values[0] = (char *)&id;
    //len[0] = sizeof(id);
    //format[0] = 1;
    ///*lastupdate*/
    //strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&cdr_pgwcdrcmb->tst_tm);
    //values[1] = timestamp;
    //len[1] = 0;
    //format[1] = 0;
    ///*pgwid*/
    //gwaddress_id = htonl(cdr_pgwcdrcmb->gwaddress_id);
    //values[2] = (char *)&gwaddress_id;
    //len[2] = sizeof(gwaddress_id);
    //format[2] = 1;
    ///*sgwid*/
    //snaddress_id = htonl(cdr_pgwcdrcmb->snaddress_id);
    //values[3] = (char *)&snaddress_id;
    //len[3] = sizeof(snaddress_id);
    //format[3] = 1;
    ///*apnid*/
    //apnId_id = htonl(cdr_pgwcdrcmb->apnId_id);
    //values[4] = (char *)&apnId_id;
    //len[4] = sizeof(apnId_id);
    //format[4] = 1;
    ///*dynamicAddressFlag*/
    //dynamicAddressFlag = cdr_pgwcdrcmb->dynamicAddressFlag;
    //values[5] = (char *)&dynamicAddressFlag;
    //len[5] = sizeof(dynamicAddressFlag);
    //format[5] = 1;
    ///*apnSelectionMode*/
    //apnSelectionMode = htons(cdr_pgwcdrcmb->apnSelectionMode);
    //values[6] = (char *)&apnSelectionMode;
    //len[6] = sizeof(apnSelectionMode);
    //format[6] = 1;
    ///*cc*/
    //cc = htonl(cdr_pgwcdrcmb->cc);
    //values[7] = (char *)&cc;
    //len[7] = sizeof(cc);
    //format[7] = 1;
    ///*chChSelectionModec*/
    //chChSelectionMode = htons(cdr_pgwcdrcmb->chChSelectionMode);
    //values[8] = (char *)&chChSelectionMode;
    //len[8] = sizeof(chChSelectionMode);
    //format[8] = 1;
    ///*iMSsignalingContext*/
    //iMSsignalingContext = cdr_pgwcdrcmb->iMSsignalingContext;
    //values[9] = (char *)&iMSsignalingContext;
    //len[9] = sizeof(iMSsignalingContext);
    //format[9] = 1;
    ///*cid*/
    //cdrCi_id = htonl(cdr_pgwcdrcmb->cdrCi_id);
    //values[10] = (char *)&cdrCi_id;
    //len[10] = sizeof(cdrCi_id);
    //format[10] = 1;
    //r = myDB_PQexecPrepare( pgCon, preparedName, PGWCDRCMB_LEN, (const char * const *)values, len, format, msgStr, sizeof(msgStr)); 
    //if( r)
    //{
    //  r = -1;
    //  snprintf(logStr,sizeof(logStr),"Error %s:DB PQexecPrepare,Result=%d,%s",__FUNCTION__,r,msgStr);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  break;
    //}
  }
  //printf("Entra aqui 1\n");
  if(!r)
  {
  //printf("Entra aqui 2\n");
    snprintf(sql,sizeof(sql),\
      "COPY %s_pgwcdrcmb(id,lastupdate,pgwid,sgwid,apnid,dynamicAddressFlag,apnSelectionMode,cc,chChSelectionMode,iMSsignalingContext,cid)\
       FROM STDIN WITH (FORMAT csv);",\
       tableName);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res) == PGRES_COPY_IN)
    {
      PQclear(res);
      if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
    }
    res = PQgetResult(pgCon->pgCon);
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      r = -1;
    }
    PQclear(res);
    //printf("LLega hasta aqui\n");
    if(!r)
    {
      snprintf(sql,sizeof(sql),"\
	WITH sel1 AS (      SELECT \
	%s_pgwcdrcmb.id AS s_pgwcdrcmb_id,\
	lastupdate AS s_lastupdate,\
	cdr_gwaddress.id AS cdr_gwaddress_id,\
	cdr_snaddress.id AS cdr_snaddress_id,\
	cdr_apn.id AS cdr_apn_id,\
	dynamicaddressflag AS s_dynamicaddressflag,\
	apnselectionmode AS s_apnselectionmode,\
	cc AS s_cc,\
	chchselectionmode AS s_chchselectionmode,\
	imssignalingcontext AS s_imssignalingcontext,\
	cdr_ci.id AS cdr_ci_id \
	FROM %s_pgwcdrcmb\
	JOIN %s_gwaddress ON %s_gwaddress.id=%s_pgwcdrcmb.pgwid    JOIN cdr_mcc AS gwaddress_mcc ON gwaddress_mcc.mcc=%s_gwaddress.mcc        JOIN cdr_mnc AS gwaddress_mnc ON gwaddress_mcc.id=gwaddress_mnc.mccid AND gwaddress_mnc.mnc=%s_gwaddress.mnc       JOIN cdr_gwaddress ON cdr_gwaddress.gwaddr=%s_gwaddress.gwaddr AND cdr_gwaddress.plmnid=gwaddress_mnc.id   JOIN %s_snaddress ON %s_snaddress.id=%s_pgwcdrcmb.sgwid JOIN cdr_mcc AS snaddress_mcc ON snaddress_mcc.mcc=%s_snaddress.mcc        JOIN cdr_mnc AS snaddress_mnc ON snaddress_mcc.id=snaddress_mnc.mccid AND snaddress_mnc.mnc=%s_snaddress.mnc       JOIN cdr_snaddress ON cdr_snaddress.snaddr=%s_snaddress.snaddr AND cdr_snaddress.sntype=%s_snaddress.sntype AND cdr_snaddress.plmnid=snaddress_mnc.id      JOIN %s_apn ON %s_apn.id=%s_pgwcdrcmb.apnid        JOIN cdr_mcc AS apn_mcc ON apn_mcc.mcc=%s_apn.mcc       JOIN cdr_mnc AS apn_mnc ON apn_mcc.id=apn_mnc.mccid AND apn_mnc.mnc=%s_apn.mnc     JOIN cdr_apn ON cdr_apn.apnni=%s_apn.apnni AND cdr_apn.plmnid=apn_mnc.id   JOIN %s_ci ON %s_ci.id=%s_pgwcdrcmb.cid JOIN cdr_mcc AS ci_mcc ON ci_mcc.mcc=%s_ci.mcc     JOIN cdr_mnc AS ci_mnc ON ci_mnc.mccid=ci_mcc.id AND ci_mnc.mnc=%s_ci.mnc  JOIN cdr_ci ON cdr_ci.ci=%s_ci.ci AND cdr_ci.lac=%s_ci.lac AND cdr_ci.rattype=%s_ci.rattype AND cdr_ci.plmnid=ci_mnc.id    ),\
      upd1 AS (UPDATE cdr_pgwcdrcmb SET lastupdate=now() FROM sel1       WHERE        pgwid=cdr_gwaddress_id       AND sgwid=cdr_snaddress_id      AND apnid=cdr_apn_id    AND dynamicaddressflag=s_dynamicaddressflag     AND apnselectionmode=s_apnselectionmode    AND cc=s_cc     AND chchselectionmode=s_chchselectionmode       AND imssignalingcontext=s_imssignalingcontext      AND cid=cdr_ci_id      RETURNING cdr_pgwcdrcmb.id AS pgwcdrcmb_id,\
   sel1.s_pgwcdrcmb_id  ),\
      ins1 AS (INSERT INTO cdr_pgwcdrcmb(tmp_id,\
  lastupdate,\
  pgwid,\
  sgwid,\
  apnid,\
  dynamicaddressflag,\
  apnselectionmode,\
  cc,\
  chchselectionmode,\
  imssignalingcontext,\
  cid)       SELECT s_pgwcdrcmb_id,\
  s_lastupdate,\
  cdr_gwaddress_id,\
  cdr_snaddress_id,\
  cdr_apn_id,\
  s_dynamicaddressflag,\
  s_apnselectionmode,\
  s_cc,\
  s_chchselectionmode,\
  s_imssignalingcontext,\
  cdr_ci_id       FROM sel1 WHERE (cdr_gwaddress_id,\
  cdr_snaddress_id,\
  cdr_apn_id,\
  s_dynamicaddressflag,\
  s_apnselectionmode,\
  s_cc,\
  s_chchselectionmode,\
  s_imssignalingcontext,\
  cdr_ci_id) NOT IN (SELECT pgwid,\
  sgwid,\
  apnid,\
  dynamicaddressflag,\
  apnselectionmode,\
  cc,\
  chchselectionmode,\
  imssignalingcontext,\
  cid FROM cdr_pgwcdrcmb)      RETURNING cdr_pgwcdrcmb.id AS pgwcdrcmb_id,\
   cdr_pgwcdrcmb.tmp_id AS s_pgwcdrcmb_id    ),\
      sel2 AS (SELECT * FROM upd1 UNION SELECT * FROM ins1)    UPDATE %s_pgwcdrcmb SET pgwcdrcmbid=pgwcdrcmb_id FROM sel2 WHERE id=s_pgwcdrcmb_id      ;\
      ",tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName);
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
	r = -1;
	snprintf(logStr,sizeof(logStr),"Error:%s:addPgwCdrCmbResult=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  free(buffer);
  return r;
}
int addCdrCi_substrk(struct _pgCon *pgCon, void *it_ciDB, char *tableName)
{
  #define CDR_CI_BUFFER 1024*1024
  char *buffer=NULL;
  uint32_t buffer_len=CDR_CI_BUFFER;
  uint32_t buffer_count=0;
  PGresult *res;
  int r=0;
  struct _cdrCi *ci=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  buffer = malloc(CDR_CI_BUFFER);
  if(!buffer)
    return -1;
  while( (ci = myDB_getNextRow( it_ciDB)))
  {
    if((buffer_len-buffer_count)<1024)
    {
      buffer = realloc(buffer,buffer_len+CDR_CI_BUFFER);
      if(buffer)
      {
        buffer_len+=CDR_CI_BUFFER;
      }
      else
        return -1;
    }
    buffer_count += snprintf(buffer+buffer_count,buffer_len-buffer_count,\
      "%u,%u,%u,%u,%u\n",\
      ci->ratType,\
      ci->uli.mcc,\
      ci->uli.mnc,\
      ci->uli.lac,\
      ci->uli.uliType.ecgi? ci->uli.eci : ci->uli.ci\
      );
    //snprintf(sql,sizeof(sql),"INSERT INTO %s_ci(rattype,mcc,mnc,lac,ci) VALUES (%d,%d,%d,%u,%u);",tableName,ci->ratType,ci->uli.mcc,ci->uli.mnc,ci->uli.lac,ci->uli.uliType.ecgi? ci->uli.eci : ci->uli.ci);
    //r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    //if( r)
    //{
    //  r = -1;
    //  snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  break;
    //}
  }
  if(!r)
  {
	snprintf(logStr,sizeof(logStr),"pn=%u, copy celltrk",pgCon->procId);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    snprintf(sql,sizeof(sql),"COPY %s_ci(rattype,mcc,mnc,lac,ci) FROM STDIN WITH (FORMAT csv)",tableName);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res) == PGRES_COPY_IN)
    {
      PQclear(res);
      if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
    }
    res = PQgetResult(pgCon->pgCon);
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      r = -1;
    }
    PQclear(res);
    if(!r)
    {
	snprintf(logStr,sizeof(logStr),"pn=%u, insert celltrk",pgCon->procId);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      snprintf(sql,sizeof(sql),"INSERT INTO cdr.celltrk(rattype,mcc,mnc,lac,ci) select rattype,mcc,mnc,lac,ci from %s_ci where (rattype,mcc,mnc,lac,ci) NOT IN (SELECT rattype,mcc,mnc,lac,ci FROM cdr.celltrk);",tableName);
      //snprintf(sql,sizeof(sql),"insert into cdr.ci(rattype,mcc,mnc,lac,ci) SELECT rattype,mcc,mnc,lac,ci FROM %s_ci ON CONFLICT (rattype,mcc,mnc,lac,ci)  DO NOTHING;",tableName);
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
	r = -1;
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  free(buffer);
  return r;
}
int addCdrCi(struct _pgCon *pgCon, void *it_ciDB, char *tableName)
{
  #define CDR_CI_BUFFER 1024*1024
  char *buffer=NULL;
  uint32_t buffer_len=CDR_CI_BUFFER;
  uint32_t buffer_count=0;
  PGresult *res;
  int r=0;
  struct _cdrCi *ci=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  buffer = malloc(CDR_CI_BUFFER);
  if(!buffer)
    return -1;
  while( (ci = myDB_getNextRow( it_ciDB)))
  {
    if((buffer_len-buffer_count)<1024)
    {
      buffer = realloc(buffer,buffer_len+CDR_CI_BUFFER);
      if(buffer)
      {
        buffer_len+=CDR_CI_BUFFER;
      }
      else
        return -1;
    }
    buffer_count += snprintf(buffer+buffer_count,buffer_len-buffer_count,\
      "%u,%u,%u,%u,%u,%u\n",\
      ci->id,\
      ci->ratType,\
      ci->uli.mcc,\
      ci->uli.mnc,\
      ci->uli.lac,\
      ci->uli.uliType.ecgi? ci->uli.eci : ci->uli.ci\
      );
    //snprintf(sql,sizeof(sql),"INSERT INTO %s_ci(id,rattype,mcc,mnc,lac,ci) VALUES (%u,%d,%d,%d,%d,%d);",tableName,ci->id,ci->ratType,ci->uli.mcc,ci->uli.mnc,ci->uli.lac,ci->uli.uliType.ecgi? ci->uli.eci : ci->uli.ci);
    //r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    //if( r)
    //{
    //  r = -1;
    //  snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  break;
    //}
  }
  if(!r)
  {
    snprintf(sql,sizeof(sql),"COPY %s_ci(id,rattype,mcc,mnc,lac,ci) FROM STDIN WITH (FORMAT csv)",tableName);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res) == PGRES_COPY_IN)
    {
      PQclear(res);
      if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
    }
    res = PQgetResult(pgCon->pgCon);
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      r = -1;
    }
    PQclear(res);
    if(!r)
    {
      snprintf(sql,sizeof(sql),"with sel1 as (select cdr_mnc.id as plmn,rattype,lac,ci FROM %s_ci JOIN cdr_mcc ON cdr_mcc.mcc=%s_ci.mcc JOIN cdr_mnc ON cdr_mnc.mnc=%s_ci.mnc WHERE cdr_mnc.mccid=cdr_mcc.id) INSERT INTO cdr_ci(plmnid,rattype,lac,ci) select sel1.plmn,sel1.rattype,sel1.lac,sel1.ci from sel1 where (sel1.plmn,sel1.rattype,sel1.lac,sel1.ci) NOT IN (SELECT plmnid,rattype,lac,ci FROM cdr_ci);",tableName,tableName,tableName);
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
	r = -1;
	snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  free(buffer);
  return r;
}
int gwAddress_tmpTable(struct _pgCon *pgCon, char *tableName)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  snprintf(sql,sizeof(sql),"\
    CREATE TEMPORARY TABLE %s_gwaddress(\
      id int primary key, \
      gwaddr inet, \
      mcc smallint, \
      mnc smallint, \
      nodeid varchar \
    ) /*ON COMMIT DELETE ROWS*/", tableName);
  r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
  if( r)
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return r;
}
int addCdrGwAddress(struct _pgCon *pgCon, void *it_gwaddressDB, char *tableName)
{
  int r=0;
  struct _gwaddress *gwaddress=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  char ipStr[128];
  while( (gwaddress = myDB_getNextRow( it_gwaddressDB)))
  {
    inet_ntop(AF_INET6,&gwaddress->gwaddress,ipStr,sizeof(ipStr));
    snprintf(sql,sizeof(sql),"INSERT INTO %s_gwaddress(id,gwaddr,mcc,mnc,nodeid) VALUES (%u,'%s',%d,%d,'%s')",tableName,gwaddress->id,ipStr,gwaddress->plmn.mcc,gwaddress->plmn.mnc,gwaddress->nodeId);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  if(!r)
  {
    snprintf(sql,sizeof(sql),"INSERT INTO cdr_gwaddress(plmnid,gwaddr,nodeid) SELECT cdr_mnc.id,gwaddr,cdr_node.id FROM %s_gwaddress JOIN cdr_mcc ON cdr_mcc.mcc=%s_gwaddress.mcc JOIN  cdr_mnc ON cdr_mnc.mccid=cdr_mcc.id AND cdr_mnc.mnc=%s_gwaddress.mnc JOIN cdr_node ON cdr_node.nodename=%s_gwaddress.nodeid WHERE (cdr_mnc.id,gwaddr,cdr_node.id) NOT IN (SELECT plmnid,gwaddr,nodeid FROM cdr_gwaddress);",tableName,tableName,tableName,tableName);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  return r;
}
int snAddress_tmpTable(struct _pgCon *pgCon, char *tableName)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  snprintf(sql,sizeof(sql),"\
    CREATE TEMPORARY TABLE %s_snaddress(\
      id int primary key, \
      snaddr inet, \
      sntype smallint, \
      mcc smallint, \
      mnc smallint \
    ) /*ON COMMIT DELETE ROWS*/", tableName);
  r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
  if( r)
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return r;
}
int addCdrSnAddress(struct _pgCon *pgCon, void *it_snaddressDB, char *tableName)
{
  int r=0;
  struct _snaddress *snaddress=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  char ipStr[128];
  while( (snaddress = myDB_getNextRow( it_snaddressDB)))
  {
    inet_ntop(AF_INET6,&snaddress->snaddress,ipStr,sizeof(ipStr));
    snprintf(sql,sizeof(sql),"INSERT INTO %s_snaddress(id,snaddr,sntype,mcc,mnc) VALUES (%u,'%s',%d,%d,%d)", tableName, snaddress->id, ipStr, snaddress->sntype, snaddress->plmn.mcc, snaddress->plmn.mnc);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  if(!r)
  {
    snprintf(sql,sizeof(sql),"INSERT INTO cdr_snaddress(plmnid,snaddr,sntype) SELECT cdr_mnc.id,snaddr,sntype FROM %s_snaddress JOIN cdr_mcc ON cdr_mcc.mcc=%s_snaddress.mcc JOIN cdr_mnc ON cdr_mnc.mccid=cdr_mcc.id AND cdr_mnc.mnc=%s_snaddress.mnc WHERE (cdr_mnc.id,snaddr,sntype) NOT IN (SELECT plmnid,snaddr,sntype FROM cdr_snaddress)",tableName,tableName,tableName);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  return r;
}
int addCdrNodeId(struct _pgCon *pgCon, void *it_nodeDB)
{
  int r=0;
  struct _nodeId *nodeId=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  while( (nodeId = myDB_getNextRow( it_nodeDB)))
  {
    snprintf(sql,sizeof(sql),"with sel1 as (select '%s' AS name, cdr_mnc.id FROM cdr_mnc FULL OUTER JOIN cdr_mcc on cdr_mnc.mccid=cdr_mcc.id WHERE cdr_mnc.mnc=%d AND cdr_mcc.mcc=%d) insert into cdr_node(nodename,plmnid) select sel1.name,sel1.id from sel1 where (sel1.name::varchar,sel1.id) NOT IN (SELECT nodename,plmnid FROM cdr_node)",nodeId->nodeName,nodeId->plmn.mnc,nodeId->plmn.mcc);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  return r;
}
int addCdrApn_substrk(struct _pgCon *pgCon, void *it_apnDB, char *tableName)
{
  int r=0;
  struct _apnId *apnId=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  while( (apnId = myDB_getNextRow( it_apnDB)))
  {
    snprintf(sql,sizeof(sql),"INSERT INTO %s_apn(apnni) VALUES ('%s')",tableName,apnId->apnNi);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  if(!r)
  {
    snprintf(sql,sizeof(sql),"INSERT INTO cdr.apntrk(name) SELECT apnni FROM %s_apn WHERE (apnni) NOT IN (SELECT name from cdr.apntrk)",tableName);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  return r;
}
int apn_tmpTable( struct _pgCon *pgCon, char *tableName)
{
  int r=0;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  snprintf(sql,sizeof(sql),"\
    CREATE TEMPORARY TABLE %s_apn(\
      id int primary key, \
      apnni varchar, \
      mcc smallint, \
      mnc smallint \
    ) /*ON COMMIT DELETE ROWS*/", tableName);
  r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
  if( r)
  {
    r = -1;
    snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return r;
}
int addCdrApn(struct _pgCon *pgCon, void *it_apnDB, char *tableName)
{
  int r=0;
  struct _apnId *apnId=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  while( (apnId = myDB_getNextRow( it_apnDB)))
  {
    snprintf(sql,sizeof(sql),"INSERT INTO %s_apn(id,apnni,mcc,mnc) VALUES (%u,'%s',%d,%d)",tableName,apnId->id,apnId->apnNi,apnId->plmn.mcc,apnId->plmn.mnc);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  if(!r)
  {
    snprintf(sql,sizeof(sql),"INSERT INTO cdr_apn(plmnid,apnni) SELECT cdr_mnc.id,apnni FROM %s_apn JOIN cdr_mcc ON cdr_mcc.mcc=%s_apn.mcc JOIN cdr_mnc ON cdr_mnc.mccid=cdr_mcc.id AND cdr_mnc.mnc=%s_apn.mnc WHERE (cdr_mnc.id,apnni) NOT IN (SELECT plmnid,apnni FROM cdr_apn)",tableName,tableName,tableName);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  return r;
}
int addCdrPlmn(struct _pgCon *pgCon, void *it_plmnDB)
{
  int r=0;
  struct _plmnId *plmn=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  while( (plmn = myDB_getNextRow( it_plmnDB)))
  {
    snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT cdr_mcc.id, %d AS mnc FROM cdr_mcc WHERE mcc=%d) INSERT INTO cdr_mnc(mccid,mnc) SELECT sel1.id,sel1.mnc FROM sel1 WHERE (sel1.id,sel1.mnc) NOT IN (SELECT cdr_mnc.mccid,cdr_mnc.mnc FROM cdr_mnc)",plmn->mnc,plmn->mcc);
    r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
    if( r)
    {
      r = -1;
      snprintf(logStr,sizeof(logStr),"Error:%s:Result=%d,%s",__FUNCTION__,r,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  return r;
}
int addCdrMcc(struct _pgCon *pgCon, void *it_mccDB)
{
  int r=0;
  uint16_t *cdrMcc=NULL;
  char sql[512];
  char msgStr[512];
  char logStr[512];
  while( (cdrMcc = myDB_getNextRow( it_mccDB)))
  {
    snprintf(sql,sizeof(sql),"INSERT INTO cdr_mcc(mcc) SELECT %d WHERE %d NOT IN (SELECT mcc FROM cdr_mcc)",*cdrMcc,*cdrMcc);
      r = myDB_PQexec( pgCon, sql, msgStr,sizeof(msgStr));
      if( r)
      {
        r = -1;
        snprintf(logStr,sizeof(logStr),"Error:addCdrMcc:Result=%d,%s",r,msgStr);
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
        break;
      }
  }
  return r;
}
/*
 * Create table for store teporary PDP data
 *
 */
int createPdpTable(struct _pgCon *pgCon, char *tableName)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"CREATE TABLE %s ( tst timestamp, apn varchar, rattype smallint, gwaddr inet, srvgwaddr inet, pdpstart bigint, pdpstop bigint, pdpupdate bigint, pdpconcurrent real);CREATE INDEX i1_%s ON %s(tst,apn,rattype,gwaddr,srvgwaddr);",tableName,tableName,tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s,%s",__func__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
int deletePdpTable(struct _pgCon *pgCon, char *tableName)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"DELETE FROM pdptables WHERE tablename='%s'; DROP TABLE %s;",tableName,tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s,%s",__func__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
int subscribePdpTable(struct _pgCon *pgCon, char *tableName)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"INSERT INTO pdptables(tablename,status) VALUES ('%s',0);",tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s:%s",__func__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
int commitPdpTable(struct _pgCon *pgCon, char *tableName)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"UPDATE pdptables SET status=1 WHERE tablename='%s';",tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s:%s",__func__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
int declinePdpTable(struct _pgCon *pgCon, char *tableName)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"UPDATE pdptables SET status=2 WHERE tablename='%s';",tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s:%s",__func__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
int pdpPrepare( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[2024];
  char msgStr[2024];
  char logStr[2024];
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT COUNT(*) AS q FROM srvnodeaddress WHERE addr=$5 and srvnodetype=$12), ins1 AS (INSERT INTO srvnodeaddress(addr,srvnodetype,mcc,mnc) SELECT $5,$12,$10,$11 FROM sel1 WHERE sel1.q=0 AND $10::smallint>0 AND $11::smallint>0) INSERT INTO pdpStat(tst,apn,rattype,gwaddr,srvgwaddr,pdpstart,pdpstop,pdpupdate,pdpconcurrent) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)");
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"%s,%s",__func__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
int pdpPrepareDealloc( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[2024];
  char msgStr[2024];
  char logStr[2024];
  snprintf(sql,sizeof(sql),"DEALLOCATE PREPARE %s;", preparedName);
  if( myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"%s,%s",__func__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/* Prepare for insert into pdp table */
int pdpToSqlPrepared(struct _pgCon *pgCon, struct _pdpStat *pdpStat, char *preparedName)
{
  int retCode=0;
  #define pdpToSql_FIELDS 12
  const char *values[pdpToSql_FIELDS];
  uint64_t pdpStart, pdpStop, pdpUpdate;
  uint16_t rATType, mcc, mnc, svrnodetype;
  char pdpConcurrent[64];
  char logStr[512], gwAddress[64], servingNodeAddr[64];
  char msgStr[512];
  char timeStr[32];
  if( pgCon && preparedName)
  {
    /*Prepared sizes*/
    const int len[pdpToSql_FIELDS]={
      0, // tst::timestamp
      0, // apn::varchar
      2, // rattype::smallint
      0, // gwaddress::inet
      0, // servingNodeAddress::inet
      8, // pdpStart::bigint
      8, // pdpStop::bigint
      8, // pdpUpdate::bigint
      0, // pdpConcurrent::real
      2, // mcc::smallint
      2, // mnc::smallint
      2  // srvnodetype::smallint
    };
    /*Prepared format*/
    const int format[pdpToSql_FIELDS]={
      0, // tst::timestamp
      0, // apn::varchar
      1, // rattype::smallint
      0, // gwaddress::inet
      0, // servingNodeAddress::inet
      1, // pdpStart::bigint
      1, // pdpStop::bigint
      1, // pdpUpdate::bigint
      0, // pdpConcurrent::real
      1, // mcc::smallint
      1, // mnc::smallint
      1  // srvnodetype::smallint
    };
    values[0] = ctime_r( &pdpStat->tst,timeStr);
    values[1] = pdpStat->apn;
    rATType = htobe16( pdpStat->rATType);
    values[2] = (char *)&rATType;
    values[3] = inet_ntop(AF_INET6,&pdpStat->gwAddr,gwAddress,sizeof(gwAddress));
    values[4] = inet_ntop(AF_INET6,&pdpStat->servingNodeAddr,servingNodeAddr,sizeof(servingNodeAddr));
    pdpStart = htobe64(pdpStat->pdpStart);
    values[5] = (char *)&pdpStart;
    pdpStop = htobe64(pdpStat->pdpStop);
    values[6] = (char *)&pdpStop;
    pdpUpdate = htobe64(pdpStat->pdpUpdate);
    values[7] = (char *)&pdpUpdate;
    //pdpConcurrent = htobe32(pdpStat->pdpConcurrent);
    snprintf(pdpConcurrent,sizeof(pdpConcurrent),"%f",pdpStat->pdpConcurrent);
    values[8] = pdpConcurrent;
    mcc = htobe16(pdpStat->mcc);
    values[9] = (char *)&mcc;
    mnc = htobe16(pdpStat->mnc);
    values[10] = (char *)&mnc;
    svrnodetype = htobe16(pdpStat->servingNodeType);
    values[11] = (char *)&svrnodetype;
    retCode = myDB_PQexecPrepare( pgCon, preparedName, pdpToSql_FIELDS, values, len, format, msgStr, sizeof(msgStr));
    if( retCode)
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"%s:%s",__func__,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  else
    retCode=-2;
  return retCode;
}
/*
 * Insert PDP data
 *
 */
int insertPdp(struct _pgCon *pgCon, char *tableName, struct _pgwRecord *pgwRecord)
{
	int retCode=0;
	char logStr[512];
	char auxStr[512];
	char sql[3072];
	snprintf(sql,sizeof(sql),"CREATE TABLE %s (recordoptime timestamp,recseqnumber int,causeforrecc smallint,duration int,msisdn varchar,imsi varchar,imei varchar,apn varchar,ip inet,mcc smallint,mnc smallint,lac int,ci int,chargingid bigint,chargingCharacteristics smallint,upoctets bigint,downoctets bigint,rattype smallint,gwaddress inet,servingNodeAddress inet,nodeid varchar) WITH autovacuum_enabled = false, toast.autovacuum_enabled = false;",tableName);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	/*DB exec error*/
	if( retCode)
	{
	  retCode = -2;
	  snprintf(logStr,sizeof(logStr),"%s:Error,retCode=%d,%s",__func__,retCode,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	return retCode;
}
/*Compact LOSD temporary tablw*/
int maintainLosdTmpTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0, retIntB=2;
  unsigned int i=0, j=0;
  char sql[50240];
  char logStr[20240];
  char auxStr[20240];
  time_t t1,t2;
  char fromStr[30000];
  char *fromStrB=fromStr;
  unsigned int fromLen=30000;
  unsigned int fromCount=0;
  char tableStr[30000];
  char *tableStrB=tableStr;
  unsigned int tableLen=30000;
  unsigned int tableCount=0;
  char *crtime;
  PGresult *res=NULL, *res2=NULL;
  //do
  //{
    strncpy(sql,"SELECT name,date_trunc('min',ctime) as crtime FROM cdr_tables WHERE ttype='losd_tmp' AND status=1 AND gran=60 ORDER BY crtime ASC",sizeof(sql));
    res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
    if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
    {
      i = 0;
      retInt = PQntuples(res);
      if(retInt)
      {
	fromCount=0;
	tableCount=0;
	for(i=0;i<retInt;i++)
	{
	  if(i==0)
	  {
	    crtime = PQgetvalue(res,0,1);
	    strncpy(tableStrB+tableCount,PQgetvalue(res,i,0),tableLen-tableCount);
	    tableCount = strlen(tableStrB);
	    fromCount += snprintf(fromStrB+fromCount,fromLen-fromCount," SELECT * FROM %s ",PQgetvalue(res,i,0));
	  }
	  else
	  {
	    if(strcmp(crtime,PQgetvalue(res,i,1)))
	    {
	      break;
	    }
	    tableCount += snprintf(tableStrB+tableCount,tableLen-tableCount,",%s",PQgetvalue(res,i,0));
	    fromCount += snprintf(fromStrB+fromCount,fromLen-fromCount," UNION ALL SELECT * FROM %s ",PQgetvalue(res,i,0));
	  }
	}
	snprintf(sql,sizeof(sql),"SELECT tname,fecha FROM (%s) AS selx GROUP BY tname,fecha;",fromStr);
	res2 = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
	if( (PQresultStatus(res2) == PGRES_TUPLES_OK) || (PQresultStatus(res2) == PGRES_SINGLE_TUPLE))
	{
	  time(&t1);
	  retIntB = PQntuples(res2);
	  for(j=0;(j<retIntB)&&!retCode;j++)
	  {
	    retCode = addLosdTableName(pgCon,PQgetvalue(res2,j,0),PQgetvalue(res2,j,1),60);
	  }
	  if(!retCode)
	  {
	    retCode = myDB_PQexec( pgCon,"BEGIN",auxStr,sizeof(auxStr));
	    if(retCode)
	    {
	      snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    }
	    else
	    {
	      for(j=0;j<retIntB;j++)
	      {
		snprintf(sql,sizeof(sql),"\
		INSERT INTO %s(\
		 apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
		 datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
		)\
		SELECT\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
		SUM(datavolumefbcuplink)                     AS datavolumefbcuplink,\
		SUM(datavolumefbcdownlink)                   AS datavolumefbcdownlink,\
		SUM(sessions)                                AS sessions,\
		SUM(hits)                                    AS hits,\
		SUM(qoschange)                               AS qoschange,\
		SUM(sgsnchange)                              AS sgsnchange,\
		SUM(sgsnplmnidchange)                        AS sgsnplmnidchange,\
		SUM(tarifftimeswitch)                        AS tarifftimeswitch,\
		SUM(pdpcontextrelease)                       AS pdpcontextrelease,\
		SUM(ratchange)                               AS ratchange,\
		SUM(serviceidledout)                         AS serviceidledout,\
		SUM(configurationchange)                     AS configurationchange,\
		SUM(servicestop)                             AS servicestop,\
		SUM(dccatimethresholdreached)                AS dccatimethresholdreached,\
		SUM(dccavolumethresholdreached)              AS dccavolumethresholdreached,\
		SUM(dccaservicespecificunitthresholdreached) AS dccaservicespecificunitthresholdreached,\
		SUM(dccatimeexhausted)                       AS dccatimeexhausted,\
		SUM(dccavolumeexhausted)                     AS dccavolumeexhausted,\
		SUM(dccavaliditytimeout)                     AS dccavaliditytimeout,\
		SUM(dccareauthorisationrequest)              AS dccareauthorisationrequest,\
		SUM(dccacontinueongoingsession)              AS dccacontinueongoingsession,\
		SUM(dccaretryandterminateongoingsession)     AS dccaretryandterminateongoingsession,\
		SUM(dccaterminateongoingsession)             AS dccaterminateongoingsession,\
		SUM(cgi_saichange)                           AS cgi_saichange,\
		SUM(raichange)                               AS raichange,\
		SUM(dccaservicespecificunitexhausted)        AS dccaservicespecificunitexhausted,\
		SUM(recordclosure)                           AS recordclosure,\
		SUM(timelimit)                               AS timelimit,\
		SUM(volumelimit)                             AS volumelimit,\
		SUM(servicespecificunitlimit)                AS servicespecificunitlimit,\
		SUM(envelopeclosure)                         AS envelopeclosure,\
		SUM(ecgichange)                              AS ecgichange,\
		SUM(taichange)                               AS taichange,\
		SUM(userlocationchange)                      AS userlocationchange\
		FROM (%s) AS selx WHERE tname='%s' GROUP BY\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue;\
                UPDATE cdr_tables SET status=1 WHERE name='%s';\
		",PQgetvalue(res2,j,0),fromStr,PQgetvalue(res2,j,0),PQgetvalue(res2,j,0));
		retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
		if(retCode)
		{
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  break;
		}
	      }
	      if(!retCode)
	      {
		snprintf(sql,sizeof(sql),"DROP TABLE %s; DELETE FROM cdr_tables WHERE (name) IN (SELECT regexp_split_to_table('%s',E','));",tableStr,tableStr);
		retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
		if( !retCode)
		{
		  time(&t2);
		  snprintf(logStr,sizeof(logStr),"RESUME/DROP TABLE %s, %u/%u, %ld sec",tableStr,i,retInt,t2-t1);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
		else
		{
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
	      }
	    }
	    myDB_PQexec( pgCon, "COMMIT", NULL, 0);
	  }
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  retCode = -1;
	}
	PQclear(res2);
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear(res);
  //}
  //while((retInt-i)&&!retCode);
  if(!retCode)
    retCode = retInt-i;
  return retCode;
}
int maintainLosdMeanTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0;
  unsigned int i=0;
  char sql[50240];
  char logStr[20240];
  char auxStr[20240];
  time_t t1,t2;
  PGresult *res=NULL, *res2=NULL;
  /*Create 20 minutes mean tables*/
  strncpy(sql,"SELECT table_days.name,\
                      to_char(table_days.time, 'HH24:MI:00') AS tod,\
		      table_days.dow AS dow,\
		      1200 AS gran\
	       FROM (SELECT 'cdr_losd_m_1200_' || to_char(t.time, 'D_HH24MI') AS name,\
	                    t.time,\
			    to_char(t.time, 'D') AS dow\
	             FROM ( SELECT generate_series(date_trunc('hour',CURRENT_TIMESTAMP - interval '7 day'), date_trunc('hour',CURRENT_TIMESTAMP)-interval '20 min', interval '20 min') AS time ) AS t\
		 ) AS table_days\
	       WHERE table_days.name NOT IN (SELECT name FROM cdr_tables)\
	       ORDER BY table_days.name;\
    ",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      /*Delete old tables*/
      snprintf(sql,sizeof(sql),"\
        CREATE TABLE %s (tod time without time zone DEFAULT '%s', dow int DEFAULT %s CHECK (tod='%s' AND dow=%s AND gran=%s)) INHERITS (cdr_losd_m);\
	CREATE INDEX i1_%s ON %s(tod,dow,apnni,gran);\
	CREATE INDEX i2_%s ON %s(gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc);\
	CREATE INDEX i3_%s ON %s(rattype,cc,crbn);\
	CREATE INDEX i4_%s ON %s(ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue);\
	INSERT INTO cdr_tables(name,gran,dow,tod,ttype) VALUES('%s',%s,%s,'%s','losd_m');\
      ",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,2),PQgetvalue(res,i,1),PQgetvalue(res,i,2),PQgetvalue(res,i,3),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,3),PQgetvalue(res,i,2),PQgetvalue(res,i,1));
      res2 = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
      if( PQresultStatus(res2) == PGRES_COMMAND_OK)
      {
        snprintf(logStr,sizeof(logStr),"CREATE TABLE %s",PQgetvalue(res,i,0));
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	PQclear(res2);
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	PQclear(res2);
        break;
      }
    }
  }
  PQclear( res);
  /*Compact 20 minute tables to 1 day table and calculate mean values*/
  strncpy(sql,"\
    SELECT name AS sec1200, \
	   'cdr_losd_86400_'||to_char(tst,'YYYYMMDD')||'0000' AS sec86400,\
	   86400 AS gran,\
	   date_trunc('day',tst) as tst86400, \
	   'cdr_losd_m_1200_'||to_char(tst,'D')||'_'||to_char(tst,'HH24MI') AS secm1200,\
	   to_char(tst,'D') AS dow,\
	   to_char(tst,'HH24:MI:00') AS tod\
      FROM cdr_tables\
      WHERE ttype='losd' AND\
	    gran=1200 AND\
	    status=1 AND\
	    CTIME<(CURRENT_TIMESTAMP - interval '1 hour') AND\
	    tst<(CURRENT_TIMESTAMP - interval '2 day')\
      ORDER BY sec1200 ASC;\
      ",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;(i<retInt)&&(!retCode)&&(cdrDecParam.decoderOn);i++)
    {
      retCode = addLosdTableName(pgCon,PQgetvalue(res,i,1),PQgetvalue(res,i,3),atoi(PQgetvalue(res,i,2)));
      if(!retCode)
      {
	retCode = myDB_PQexec(pgCon,"BEGIN",auxStr,sizeof(auxStr));
	if(!retCode)
	{
	  time(&t1);
	  snprintf(sql,sizeof(sql),"\
	    CREATE TEMPORARY TABLE losd_tmp(LIKE losd_tpl);\
	    INSERT INTO losd_tmp(\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	    )SELECT\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      sum(datavolumefbcuplink),sum(datavolumefbcdownlink),sum(sessions),sum(hits),sum(qoschange),sum(sgsnchange),sum(sgsnplmnidchange),sum(tarifftimeswitch),sum(pdpcontextrelease),sum(ratchange),sum(serviceidledout),sum(configurationchange),sum(servicestop),sum(dccatimethresholdreached),sum(dccavolumethresholdreached),sum(dccaservicespecificunitthresholdreached),sum(dccatimeexhausted),sum(dccavolumeexhausted),sum(dccavaliditytimeout),sum(dccareauthorisationrequest),sum(dccacontinueongoingsession),sum(dccaretryandterminateongoingsession),sum(dccaterminateongoingsession),sum(cgi_saichange),sum(raichange),sum(dccaservicespecificunitexhausted),sum(recordclosure),sum(timelimit),sum(volumelimit),sum(servicespecificunitlimit),sum(envelopeclosure),sum(ecgichange),sum(taichange),sum(userlocationchange)\
	      FROM ( SELECT\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
		datavolumefbcuplink,datavolumefbcdownlink,(sessions/72) AS sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	      FROM %s UNION ALL SELECT\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
		datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	      FROM %s) AS selx GROUP BY\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue;\
	    TRUNCATE TABLE %s;\
	    INSERT INTO %s(\
	      gran,\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	    )SELECT\
	      %s,\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	      FROM losd_tmp;\
	    TRUNCATE TABLE losd_tmp;\
	    INSERT INTO losd_tmp(\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	    )SELECT\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      sum(datavolumefbcuplink),sum(datavolumefbcdownlink),sum(sessions),sum(hits),sum(qoschange),sum(sgsnchange),sum(sgsnplmnidchange),sum(tarifftimeswitch),sum(pdpcontextrelease),sum(ratchange),sum(serviceidledout),sum(configurationchange),sum(servicestop),sum(dccatimethresholdreached),sum(dccavolumethresholdreached),sum(dccaservicespecificunitthresholdreached),sum(dccatimeexhausted),sum(dccavolumeexhausted),sum(dccavaliditytimeout),sum(dccareauthorisationrequest),sum(dccacontinueongoingsession),sum(dccaretryandterminateongoingsession),sum(dccaterminateongoingsession),sum(cgi_saichange),sum(raichange),sum(dccaservicespecificunitexhausted),sum(recordclosure),sum(timelimit),sum(volumelimit),sum(servicespecificunitlimit),sum(envelopeclosure),sum(ecgichange),sum(taichange),sum(userlocationchange)\
	      FROM %s GROUP BY\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue;\
	    UPDATE %s AS losd_m SET\
	      datavolumefbcuplink                     =((losd_m.datavolumefbcuplink*3)                     + losd_tmp.datavolumefbcuplink)/4,\
	      datavolumefbcdownlink                   =((losd_m.datavolumefbcdownlink*3)                   + losd_tmp.datavolumefbcdownlink)/4,\
	      sessions                                =((losd_m.sessions*3)                                + losd_tmp.sessions)/4,\
	      hits                                    =((losd_m.hits*3)                                    + losd_tmp.hits)/4,\
	      qoschange                               =((losd_m.qoschange*3)                               + losd_tmp.qoschange)/4,\
	      sgsnchange                              =((losd_m.sgsnchange*3)                              + losd_tmp.sgsnchange)/4,\
	      sgsnplmnidchange                        =((losd_m.sgsnplmnidchange*3)                        + losd_tmp.sgsnplmnidchange)/4,\
	      tarifftimeswitch                        =((losd_m.tarifftimeswitch*3)                        + losd_tmp.tarifftimeswitch)/4,\
	      pdpcontextrelease                       =((losd_m.pdpcontextrelease*3)                       + losd_tmp.pdpcontextrelease)/4,\
	      ratchange                               =((losd_m.ratchange*3)                               + losd_tmp.ratchange)/4,\
	      serviceidledout                         =((losd_m.serviceidledout*3)                         + losd_tmp.serviceidledout)/4,\
	      configurationchange                     =((losd_m.configurationchange*3)                     + losd_tmp.configurationchange)/4,\
	      servicestop                             =((losd_m.servicestop*3)                             + losd_tmp.servicestop)/4,\
	      dccatimethresholdreached                =((losd_m.dccatimethresholdreached*3)                + losd_tmp.dccatimethresholdreached)/4,\
	      dccavolumethresholdreached              =((losd_m.dccavolumethresholdreached*3)              + losd_tmp.dccavolumethresholdreached)/4,\
	      dccaservicespecificunitthresholdreached =((losd_m.dccaservicespecificunitthresholdreached*3) + losd_tmp.dccaservicespecificunitthresholdreached)/4,\
	      dccatimeexhausted                       =((losd_m.dccatimeexhausted*3)                       + losd_tmp.dccatimeexhausted)/4,\
	      dccavolumeexhausted                     =((losd_m.dccavolumeexhausted*3)                     + losd_tmp.dccavolumeexhausted)/4,\
	      dccavaliditytimeout                     =((losd_m.dccavaliditytimeout*3)                     + losd_tmp.dccavaliditytimeout)/4,\
	      dccareauthorisationrequest              =((losd_m.dccareauthorisationrequest*3)              + losd_tmp.dccareauthorisationrequest)/4,\
	      dccacontinueongoingsession              =((losd_m.dccacontinueongoingsession*3)              + losd_tmp.dccacontinueongoingsession)/4,\
	      dccaretryandterminateongoingsession     =((losd_m.dccaretryandterminateongoingsession*3)     + losd_tmp.dccaretryandterminateongoingsession)/4,\
	      dccaterminateongoingsession             =((losd_m.dccaterminateongoingsession*3)             + losd_tmp.dccaterminateongoingsession)/4,\
	      cgi_saichange                           =((losd_m.cgi_saichange*3)                           + losd_tmp.cgi_saichange)/4,\
	      raichange                               =((losd_m.raichange*3)                               + losd_tmp.raichange)/4,\
	      dccaservicespecificunitexhausted        =((losd_m.dccaservicespecificunitexhausted*3)        + losd_tmp.dccaservicespecificunitexhausted)/4,\
	      recordclosure                           =((losd_m.recordclosure*3)                           + losd_tmp.recordclosure)/4,\
	      timelimit                               =((losd_m.timelimit*3)                               + losd_tmp.timelimit)/4,\
	      volumelimit                             =((losd_m.volumelimit*3)                             + losd_tmp.volumelimit)/4,\
	      servicespecificunitlimit                =((losd_m.servicespecificunitlimit*3)                + losd_tmp.servicespecificunitlimit)/4,\
	      envelopeclosure                         =((losd_m.envelopeclosure*3)                         + losd_tmp.envelopeclosure)/4,\
	      ecgichange                              =((losd_m.ecgichange*3)                              + losd_tmp.ecgichange)/4,\
	      taichange                               =((losd_m.taichange*3)                               + losd_tmp.taichange)/4,\
	      userlocationchange                      =((losd_m.userlocationchange*3)                      + losd_tmp.userlocationchange)/4\
	    FROM losd_tmp WHERE\
	      losd_m.apnni                    = losd_tmp.apnni AND\
	      losd_m.gwaddr                   = losd_tmp.gwaddr AND\
	      losd_m.gwmcc                    = losd_tmp.gwmcc AND\
	      losd_m.gwmnc                    = losd_tmp.gwmnc AND\
	      losd_m.snaddr                   = losd_tmp.snaddr AND\
	      losd_m.snmcc                    = losd_tmp.snmcc AND\
	      losd_m.snmnc                    = losd_tmp.snmnc AND\
	      losd_m.sntype                   = losd_tmp.sntype AND\
	      losd_m.cc                       = losd_tmp.cc AND\
	      losd_m.rattype                  = losd_tmp.rattype AND\
	      losd_m.crbn                     = losd_tmp.crbn AND\
	      losd_m.ratinggroup              = losd_tmp.ratinggroup AND\
	      losd_m.serviceidentifier        = losd_tmp.serviceidentifier AND\
	      losd_m.resultcode               = losd_tmp.resultcode AND\
	      losd_m.failurehandlingcontinue  = losd_tmp.failurehandlingcontinue;\
	    INSERT INTO %s (\
	      apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	      datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	      )\
	      SELECT\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
		datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	      FROM losd_tmp\
	      WHERE (\
		apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue\
		)\
		NOT IN (SELECT\
		  apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue\
		FROM %s GROUP BY\
		  apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue\
	      );\
	      DROP TABLE losd_tmp;\
	      DROP TABLE %s;\
	      DELETE FROM cdr_tables WHERE name='%s';\
	      UPDATE cdr_tables SET status=1 WHERE name='%s';\
	    ",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,1),PQgetvalue(res,i,1),PQgetvalue(res,i,2),PQgetvalue(res,i,0),PQgetvalue(res,i,4),PQgetvalue(res,i,4),PQgetvalue(res,i,4),PQgetvalue(res,i,0),PQgetvalue(res,i,0),PQgetvalue(res,i,1));
	  retCode = myDB_PQexec(pgCon,sql,auxStr,sizeof(auxStr));
	  if(!retCode)
	  {
	    time(&t2);
	    snprintf(logStr,sizeof(logStr),"DROP TABLE %s; UPDATE %s & %s, %u/%u, %ld sec",PQgetvalue(res,i,0),PQgetvalue(res,i,1),PQgetvalue(res,i,4),i+1,retInt,t2-t1);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  }
	  else
	  {
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
	    snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  }
	}
	else
	{
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	myDB_PQexec(pgCon,"COMMIT",NULL,0);
      }
    }
  }
  else
  {
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
    snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
    retCode = 1;
  }
  PQclear( res);
  return retCode;
}
/*Create SQL tables for losd*/
int maintainLosdTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0;
  unsigned int i=0;
  char sql[20240];
  char logStr[20240];
  char auxStr[20240];
  PGresult *res=NULL;
  char fromStr[30000];
  unsigned int fromLen=30000;
  unsigned int fromCount=0;
  char tableStr[30000];
  unsigned int tableLen=30000;
  unsigned int tableCount=0;
  char *sec1200=NULL;
  time_t t1,t2;
  /*Resume minute tables to 20 minutes table*/
  strncpy(sql,"SELECT name AS sec60,'cdr_losd_1200_'||to_char(t2,'YYYYMMDDHH24MI') as sec1200,t2,t2+interval '20 min' AS t3,1200 as gran FROM cdr_tables,date_trunc('min',tst - make_interval(0,0,0,0,0, date_part('min',tst)::int%20)) AS t2 WHERE ttype='losd' AND gran=60 AND ctime<(current_timestamp - interval '1 hour') AND status=1 ORDER BY sec60 ASC;",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    if(retInt)
    {
      fromCount=0;
      tableCount=0;
      for(i=0;i<retInt;i++)
      {
	retCode = addLosdTableName(pgCon,PQgetvalue(res,i,1),PQgetvalue(res,i,2),atoi(PQgetvalue(res,i,4)));
	if(retCode)
	  break;
	if(i==0)
	{
	  sec1200 = PQgetvalue(res,i,1);
	  tableLen = sizeof(tableStr);
	  strncpy(tableStr,PQgetvalue(res,i,0),tableLen);
	  tableCount = strlen(tableStr);
	  fromLen = sizeof(tableStr);
	  fromCount = snprintf(fromStr,fromLen,"SELECT * FROM %s ",PQgetvalue(res,i,0));
	}
	else
	{
	  if(strcmp(PQgetvalue(res,i,1),sec1200))
	  {
	    break;
	  }
	  tableCount += snprintf(tableStr+tableCount,tableLen-tableCount,",%s",PQgetvalue(res,i,0));
	  fromCount += snprintf(fromStr+fromCount,fromLen-fromCount," UNION ALL SELECT * FROM %s ",PQgetvalue(res,i,0));
	}
      }
      if(!retCode)
      {
	time(&t1);
	snprintf(sql,sizeof(sql),"\
	  INSERT INTO %s(\
            gran,\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	    datavolumefbcuplink,datavolumefbcdownlink,sessions,hits,qoschange,sgsnchange,sgsnplmnidchange,tarifftimeswitch,pdpcontextrelease,ratchange,serviceidledout,configurationchange,servicestop,dccatimethresholdreached,dccavolumethresholdreached,dccaservicespecificunitthresholdreached,dccatimeexhausted,dccavolumeexhausted,dccavaliditytimeout,dccareauthorisationrequest,dccacontinueongoingsession,dccaretryandterminateongoingsession,dccaterminateongoingsession,cgi_saichange,raichange,dccaservicespecificunitexhausted,recordclosure,timelimit,volumelimit,servicespecificunitlimit,envelopeclosure,ecgichange,taichange,userlocationchange\
	  )\
	  SELECT\
            1200,\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,\
	    SUM(datavolumefbcuplink)                     AS datavolumefbcuplink,\
	    SUM(datavolumefbcdownlink)                   AS datavolumefbcdownlink,\
	    SUM(sessions)                                AS sessions,\
	    SUM(hits)                                    AS hits,\
	    SUM(qoschange)                               AS qoschange,\
	    SUM(sgsnchange)                              AS sgsnchange,\
	    SUM(sgsnplmnidchange)                        AS sgsnplmnidchange,\
	    SUM(tarifftimeswitch)                        AS tarifftimeswitch,\
	    SUM(pdpcontextrelease)                       AS pdpcontextrelease,\
	    SUM(ratchange)                               AS ratchange,\
	    SUM(serviceidledout)                         AS serviceidledout,\
	    SUM(configurationchange)                     AS configurationchange,\
	    SUM(servicestop)                             AS servicestop,\
	    SUM(dccatimethresholdreached)                AS dccatimethresholdreached,\
	    SUM(dccavolumethresholdreached)              AS dccavolumethresholdreached,\
	    SUM(dccaservicespecificunitthresholdreached) AS dccaservicespecificunitthresholdreached,\
	    SUM(dccatimeexhausted)                       AS dccatimeexhausted,\
	    SUM(dccavolumeexhausted)                     AS dccavolumeexhausted,\
	    SUM(dccavaliditytimeout)                     AS dccavaliditytimeout,\
	    SUM(dccareauthorisationrequest)              AS dccareauthorisationrequest,\
	    SUM(dccacontinueongoingsession)              AS dccacontinueongoingsession,\
	    SUM(dccaretryandterminateongoingsession)     AS dccaretryandterminateongoingsession,\
	    SUM(dccaterminateongoingsession)             AS dccaterminateongoingsession,\
	    SUM(cgi_saichange)                           AS cgi_saichange,\
	    SUM(raichange)                               AS raichange,\
	    SUM(dccaservicespecificunitexhausted)        AS dccaservicespecificunitexhausted,\
	    SUM(recordclosure)                           AS recordclosure,\
	    SUM(timelimit)                               AS timelimit,\
	    SUM(volumelimit)                             AS volumelimit,\
	    SUM(servicespecificunitlimit)                AS servicespecificunitlimit,\
	    SUM(envelopeclosure)                         AS envelopeclosure,\
	    SUM(ecgichange)                              AS ecgichange,\
	    SUM(taichange)                               AS taichange,\
	    SUM(userlocationchange)                      AS userlocationchange\
	  FROM (%s) AS selx\
	  GROUP BY\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,snmcc,snmnc,sntype,cc,rattype,crbn,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue\
	  ;\
	  DROP TABLE %s;\
          DELETE FROM cdr_tables WHERE (name) IN (SELECT regexp_split_to_table('%s',E','));\
          UPDATE cdr_tables SET status=1 WHERE name='%s';\
          ",sec1200,fromStr,tableStr,tableStr,sec1200);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	if( !retCode)
	{
	  time(&t2);
	  snprintf(logStr,sizeof(logStr),"RESUME/DROP TABLE %s to %s, %u/%u, %ld",tableStr,sec1200,i,retInt,t2-t1);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
  }
  PQclear( res);
  if(!retCode)
  {
    if((retInt-i)>0)
      retCode = 1;
  }
  return retCode;
}
/*Create SQL tables for lotv*/
int maintainLotvTmpTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0, retIntB=2;
  unsigned int i=0, j=0;
  char sql[50240];
  char logStr[20240];
  char auxStr[20240];
  time_t t1,t2;
  char fromStr[30000];
  char *fromStrB=fromStr;
  unsigned int fromLen=30000;
  unsigned int fromCount=0;
  char tableStr[30000];
  char *tableStrB=tableStr;
  unsigned int tableLen=30000;
  unsigned int tableCount=0;
  char *crtime;
  PGresult *res=NULL, *res2=NULL;
  //do
  //{
    strncpy(sql,"SELECT name,date_trunc('min',ctime) as crtime FROM cdr_tables WHERE ttype='lotv_tmp' AND status=1 AND gran=60 ORDER BY crtime ASC",sizeof(sql));
    res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
    if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
    {
      i = 0;
      retInt = PQntuples(res);
      if(retInt)
      {
	fromCount=0;
	tableCount=0;
	for(i=0;i<retInt;i++)
	{
	  if(i==0)
	  {
	    crtime = PQgetvalue(res,0,1);
	    strncpy(tableStrB+tableCount,PQgetvalue(res,i,0),tableLen-tableCount);
	    tableCount = strlen(tableStrB);
	    fromCount += snprintf(fromStrB+fromCount,fromLen-fromCount," SELECT *\
	    FROM %s ",PQgetvalue(res,i,0));
	    // tname,fecha,
	    // apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,
	    // datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent
	  }
	  else
	  {
	    if(strcmp(crtime,PQgetvalue(res,i,1)))
	    {
	      break;
	    }
	    tableCount += snprintf(tableStrB+tableCount,tableLen-tableCount,",%s",PQgetvalue(res,i,0));
	    fromCount += snprintf(fromStrB+fromCount,fromLen-fromCount," UNION ALL SELECT *\
	    FROM %s ",PQgetvalue(res,i,0));
	    // tname,fecha,
	    // apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,
	    // datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent
	  }
	}
	snprintf(sql,sizeof(sql),"SELECT 'cdr_lotv_60_'||to_char(date_trunc('hour',fecha),'YYYYMMDDHH24') AS tname,date_trunc('hour',fecha) AS fechaday FROM (%s) AS selx GROUP BY fechaday;",fromStr);
	res2 = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
	if( (PQresultStatus(res2) == PGRES_TUPLES_OK) || (PQresultStatus(res2) == PGRES_SINGLE_TUPLE))
	{
	  time(&t1);
	  retIntB = PQntuples(res2);
	  for(j=0;(j<retIntB)&&!retCode;j++)
	  {
	    retCode = addLotvTableName(pgCon,PQgetvalue(res2,j,0),PQgetvalue(res2,j,1),60,3600);
	  }
	  if(!retCode)
	  {
	    //retCode = myDB_PQexec( pgCon,"CREATE TEMPORARY TABLE lotv_tmp( LIKE lotv_tpl)",auxStr,sizeof(auxStr));
	    retCode = myDB_PQexec( pgCon,"BEGIN",auxStr,sizeof(auxStr));
	    if(retCode)
	    {
	      snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    }
	    else
	    {
	      for(j=0;j<retIntB;j++)
	      {
		snprintf(sql,sizeof(sql),"\
		INSERT INTO %s(\
		 fecha,gran,\
		 apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,\
		 datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent\
		)\
		SELECT\
		 fecha,60,\
		 apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,\
		 sum(datavolumegprsuplink),sum(datavolumegprsdownlink),sum(qoSChange),sum(tariffTime),sum(recordClosure),sum(cGI_SAICHange),sum(rAIChange),sum(dT_Establishment),sum(dT_Removal),sum(eCGIChange),sum(tAIChange),sum(userLocationChange),sum(pdpStart),sum(pdpNormalStop),sum(pdpAbnormalStop),sum(pdpconcurrent)\
		FROM (%s) AS selx WHERE date_trunc('hour',fecha)='%s' GROUP BY\
		 fecha,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic;\
                UPDATE cdr_tables SET status=1 WHERE name='%s';\
		",PQgetvalue(res2,j,0),fromStr,PQgetvalue(res2,j,1),PQgetvalue(res2,j,0));
		retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
		if(retCode)
		{
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  break;
		}
	      }
	      if(!retCode)
	      {
		snprintf(sql,sizeof(sql),"DROP TABLE %s; DELETE FROM cdr_tables WHERE (name) IN (SELECT regexp_split_to_table('%s',E','));",tableStr,tableStr);
		retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
		if( !retCode)
		{
		  time(&t2);
		  snprintf(logStr,sizeof(logStr),"RESUME/DROP TABLE %s, %u/%u, %ld sec",tableStr,i,retInt,t2-t1);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
		else
		{
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
	      }
	    }
	    myDB_PQexec( pgCon, "COMMIT", NULL, 0);
	  }
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  retCode = -1;
	}
	PQclear(res2);
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear(res);
  //}
  //while((retInt-i)&&!retCode);
  if(!retCode)
  {
    if(retInt-i)
      retCode=1;
  }
  return retCode;
}
int maintainLotvMeanTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0;
  unsigned int i=0;
  char sql[50240];
  char logStr[20240];
  char auxStr[20240];
  char mTableName[64];
  time_t t1,t2;
  PGresult *res=NULL,*res2=NULL;
  /*Compact 20 minute tables to 1 day table and calculate mean values*/
  strncpy(sql,"\
    SELECT name AS sec1200, \
	   'cdr_lotv_86400_'||to_char(tst,'YYYYMMDD') AS sec86400,\
	   date_trunc('day',tst) as tst86400, \
	   to_char(tst,'D') AS dow\
      FROM cdr_tables\
      WHERE ttype='lotv' AND\
	    gran=1200 AND\
	    status=1 AND\
	    CTIME<(CURRENT_TIMESTAMP - interval '1 hour') AND\
	    tst<(CURRENT_TIMESTAMP - interval '2 day')\
      ORDER BY tst ASC;\
      ",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;(i<retInt)&&(!retCode)&&(cdrDecParam.decoderOn);i++)
    {
      retCode = addLotvTableName(pgCon,PQgetvalue(res,i,1),PQgetvalue(res,i,2),86400,86400);
      if(retCode) break;
      retCode = addLotvTableMeanName(pgCon,mTableName,sizeof(mTableName),atoi(PQgetvalue(res,i,3)),1200);
      if(retCode) break;
      retCode = myDB_PQexec(pgCon,"BEGIN",auxStr,sizeof(auxStr));
      /*update daily table*/
      if(!retCode)
      {
	time(&t1);
	snprintf(sql,sizeof(sql),"\
	  INSERT INTO %s(\
	    gran,\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,\
	    datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent\
	  )SELECT\
	    86400,\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,\
	    SUM(datavolumegprsuplink),SUM(datavolumegprsdownlink),SUM(qoSChange),SUM(tariffTime),SUM(recordClosure),SUM(cGI_SAICHange),SUM(rAIChange),SUM(dT_Establishment),SUM(dT_Removal),SUM(eCGIChange),SUM(tAIChange),SUM(userLocationChange),SUM(pdpStart),SUM(pdpNormalStop),SUM(pdpAbnormalStop),SUM(pdpconcurrent)/72\
	  FROM %s GROUP BY\
	    apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic;\
	    UPDATE cdr_tables SET status=1 WHERE name IN ('%s');\
	  ",PQgetvalue(res,i,1),PQgetvalue(res,i,0),PQgetvalue(res,i,1));
	res2 = myDB_getSQL(pgCon,sql,auxStr,sizeof(auxStr));
	if(PQresultStatus(res) == PGRES_COMMAND_OK)
	{
	  time(&t2);
	  snprintf(logStr,sizeof(logStr),"UPDATE %s, %d rows insert, %ld sec",PQgetvalue(res,i,1),atoi(PQcmdTuples(res2)),t2-t1);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	else
	{
	  retCode=-1;
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	PQclear(res2);
      }
      else
      {
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
	snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      /* Mean table*/
      if(!retCode)
      {
	time(&t1);
	//snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT DISTINCT ON (tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic) tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic FROM %s WHERE lastupdate>(CURRENT_TIMESTAMP - interval '28 days')), sel2 AS (SELECT now() AS lastupdate,to_char(fecha,'HH24:MI:00')::time AS tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, SUM(datavolumegprsuplink),SUM(datavolumegprsdownlink),SUM(qoSChange),SUM(tariffTime),SUM(recordClosure),SUM(cGI_SAICHange),SUM(rAIChange),SUM(dT_Establishment),SUM(dT_Removal),SUM(eCGIChange),SUM(tAIChange),SUM(userLocationChange),SUM(pdpStart),SUM(pdpNormalStop),SUM(pdpAbnormalStop),SUM(pdpconcurrent) FROM %s WHERE ( to_char(fecha,'HH24:MI:00')::time,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,) NOT IN (SELECT sel1.* FROM sel1A) GROUP BY tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,), sel3 AS (SELECT now() AS lastupdate,tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, SUM(datavolumegprsuplink)/4,SUM(datavolumegprsdownlink)/4,SUM(qoSChange)/4,SUM(tariffTime)/4,SUM(recordClosure)/4,SUM(cGI_SAICHange)/4,SUM(rAIChange)/4,SUM(dT_Establishment)/4,SUM(dT_Removal)/4,SUM(eCGIChange)/4,SUM(tAIChange)/4,SUM(userLocationChange)/4,SUM(pdpStart)/4,SUM(pdpNormalStop)/4,SUM(pdpAbnormalStop)/4,SUM(pdpconcurrent)/4 FROM (SELECT tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, (datavolumegprsuplink*3) AS datavolumegprsuplink,(datavolumegprsdownlink*3) AS idatavolumegprsdownlink,(qoSChange*3) AS qoSChange,(tariffTime*3) AS tariffTime,(recordClosure*3) AS recordClosure,(cGI_SAICHange*3) AS cGI_SAICHange,(rAIChange*3) AS rAIChange,(dT_Establishment*3) AS dT_Establishment,(dT_Removal*3) AS dT_Removal,(eCGIChange*3) AS eCGIChange,(tAIChange*3) AS tAIChange,(userLocationChange*3) AS userLocationChange,(pdpStart*3) AS pdpStart,(pdpNormalStop*3) AS pdpNormalStop,(pdpAbnormalStop*3) AS pdpAbnormalStop,(pdpconcurrent*3) AS pdpconcurrent FROM %s WHERE lastupdate>(CURRENT_TIMESTAMP - interval '28 days') UNION ALL SELECT to_char(fecha,'HH24:MI:00')::time AS tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent FROM %s ) AS selx WHERE ( tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,) IN (SELECT sel1.* FROM sel1) GROUP BY tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic,), sel4 AS (SELECT lastupdate,tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent FROM %s WHERE lastupdate>(CURRENT_TIMESTAMP - interval '28 days')), INSERT INTO %s ( lastupdate,tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent) SELECT now(),to_char(fecha,'HH24:MI:00')::time AS tod, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, SUM(datavolumegprsuplink),SUM(datavolumegprsdownlink),SUM(qoSChange),SUM(tariffTime),SUM(recordClosure),SUM(cGI_SAICHange),SUM(rAIChange),SUM(dT_Establishment),SUM(dT_Removal),SUM(eCGIChange),SUM(tAIChange),SUM(userLocationChange),SUM(pdpStart),SUM(pdpNormalStop),SUM(pdpAbnormalStop),SUM(pdpconcurrent) FROM %s WHERE (to_char(fecha,'HH24:MI:00')::time,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic) NOT IN (SELECT DISTINCT ON ( tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic) tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic FROM %s) GROUP BY tod,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic;",mTableName,PQgetvalue(res,i,0),mTableName);
	res2 = myDB_getSQL(pgCon,sql,auxStr,sizeof(auxStr));
	if(PQresultStatus(res) == PGRES_COMMAND_OK)
	{
	  time(&t2);
	    snprintf(logStr,sizeof(logStr),"INSERT TABLE %s; %d rows, %ld sec",mTableName,atoi(PQcmdTuples(res2)),t2-t1);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	else
	{
	  retCode = -1;
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	PQclear(res2);
      }
      else
      {
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
	snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      myDB_PQexec(pgCon,"COMMIT",NULL,0);
    }
  }
  else
  {
    snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  PQclear( res);
  return retCode;
	  /*
	  UPDATE %s AS lotv_m SET\
	    datavolumegprsuplink   =((lotv_m.datavolumegprsuplink*3)  + lotv_tmp.datavolumegprsuplink)/4,\
	    datavolumegprsdownlink=(( lotv_m.datavolumegprsdownlink*3)+ lotv_tmp.datavolumegprsdownlink)/4,\
	    qoSChange=((              lotv_m.qoSChange*3)             + lotv_tmp.qoSChange)/4,\
	    tariffTime=((             lotv_m.tariffTime*3)            + lotv_tmp.tariffTime)/4,\
	    recordClosure=((          lotv_m.recordClosure*3)         + lotv_tmp.recordClosure)/4,\
	    cGI_SAICHange=((          lotv_m.cGI_SAICHange*3)         + lotv_tmp.cGI_SAICHange)/4,\
	    rAIChange=((              lotv_m.rAIChange*3)             + lotv_tmp.rAIChange)/4,\
	    dT_Establishment=((       lotv_m.dT_Establishment*3)      + lotv_tmp.dT_Establishment)/4,\
	    dT_Removal=((             lotv_m.dT_Removal*3)            + lotv_tmp.dT_Removal)/4,\
	    eCGIChange=((             lotv_m.eCGIChange*3)            + lotv_tmp.eCGIChange)/4,\
	    tAIChange=((              lotv_m.tAIChange*3)             + lotv_tmp.tAIChange)/4,\
	    userLocationChange=((     lotv_m.userLocationChange*3)    + lotv_tmp.userLocationChange)/4,\
	    pdpStart=((               lotv_m.pdpStart*3)              + lotv_tmp.pdpStart)/4,\
	    pdpNormalStop=((          lotv_m.pdpNormalStop*3)         + lotv_tmp.pdpNormalStop)/4,\
	    pdpAbnormalStop=((        lotv_m.pdpAbnormalStop*3)       + lotv_tmp.pdpAbnormalStop)/4,\
	    pdpconcurrent=((          lotv_m.pdpconcurrent*3)         + lotv_tmp.pdpconcurrent)/4\
	  FROM %s AS lotv_tmp WHERE\
	    lotv_m.tod                 = to_char(lotv_tmp.fecha,'HH24:MI:00')::time AND\
	    lotv_m.apnni               = lotv_tmp.apnni AND\
	    lotv_m.gwaddr              = lotv_tmp.gwaddr AND\
	    lotv_m.gwmcc               = lotv_tmp.gwmcc AND\
	    lotv_m.gwmnc               = lotv_tmp.gwmnc AND\
	    lotv_m.snaddr              = lotv_tmp.snaddr AND\
	    lotv_m.sntype              = lotv_tmp.sntype AND\
	    lotv_m.snmcc               = lotv_tmp.snmcc AND\
	    lotv_m.snmnc               = lotv_tmp.snmnc AND\
	    lotv_m.rattype             = lotv_tmp.rattype AND\
	    lotv_m.mcc                 = lotv_tmp.mcc AND\
	    lotv_m.mnc                 = lotv_tmp.mnc AND\
	    lotv_m.lac                 = lotv_tmp.lac AND\
	    lotv_m.ci                  = lotv_tmp.ci AND\
	    lotv_m.cc                  = lotv_tmp.cc AND\
	    lotv_m.dynamicaddressflag  = lotv_tmp.dynamicaddressflag AND\
	    lotv_m.apnselectionmode    = lotv_tmp.apnselectionmode AND\
	    lotv_m.chchselectionmode   = lotv_tmp.chchselectionmode AND\
	    lotv_m.imssignalingcontext = lotv_tmp.imssignalingcontext AND\
	    lotv_m.withtraffic         = lotv_tmp.withtraffic;\
	    */
}
int maintainLotvTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0;
  unsigned int i=0;
  char sql[50240];
  char logStr[20240];
  char auxStr[20240];
  time_t t1,t2;
  PGresult *res=NULL, *res2=NULL;
  char fromStr[30000];
  unsigned int fromLen=30000;
  char tableStr[30000];
  unsigned int tableLen=30000;
  char *sec1200=NULL;
  /*Delete trailing tables*/
  strncpy(sql,"SELECT name FROM cdr_tables WHERE ttype='lotv' AND tst<(CURRENT_TIMESTAMP-interval '1 year');",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"DROP TABLE %s; DELETE FROM cdr_tables WHERE name='%s'",PQgetvalue(res,i,0),PQgetvalue(res,i,0));
      res2 = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
      if( PQresultStatus(res2) == PGRES_COMMAND_OK)
      {
        snprintf(logStr,sizeof(logStr),"DROP TABLE %s",PQgetvalue(res,i,0));
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	PQclear(res2);
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	PQclear(res2);
        break;
      }
    }
  }
  else
  {
    snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode=-1;
  }
  PQclear( res);
  /*Resume minute tables to 20 minutes table*/
  strncpy(sql,"SELECT name AS sec60,\
                      'cdr_lotv_1200_'||to_char(t2,'YYYYMMDD') as sec1200,\
		      t2,\
		      t2+interval '20 min' AS t3,\
		      1200 as gran,\
		      date_trunc('day',t2) AS t4\
	       FROM cdr_tables,\
	            date_trunc('min',tst - make_interval(0,0,0,0,0, date_part('min',tst)::int%20)) AS t2\
	       WHERE ttype='lotv' AND\
	             gran=60 AND\
		     tst<(current_timestamp - interval '1 hour') AND\
		     ctime<(current_timestamp - interval '45 min') AND\
		     status=1\
	       ORDER BY sec60 ASC;\
    ",sizeof(sql));
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    if(retInt)
    {
      /*UPDATE AND DELETE old tables*/
      i=0;
      retCode = addLotvTableName(pgCon,PQgetvalue(res,i,1),PQgetvalue(res,i,5),atoi(PQgetvalue(res,i,4)),86400);
      if(!retCode)
      {
	sec1200 = PQgetvalue(res,i,1);
	tableLen = sizeof(tableStr);
	strncpy(tableStr,PQgetvalue(res,i,0),tableLen);
	//tableCount = strlen(tableStr);
	fromLen = sizeof(tableStr);
	snprintf(fromStr,fromLen,"SELECT * FROM %s",PQgetvalue(res,i,0));
	time(&t1);
	snprintf(sql,sizeof(sql),"INSERT INTO %s( gran, fecha, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, datavolumegprsuplink,datavolumegprsdownlink,qoSChange,tariffTime,recordClosure,cGI_SAICHange,rAIChange,dT_Establishment,dT_Removal,eCGIChange,tAIChange,userLocationChange,pdpStart,pdpNormalStop,pdpAbnormalStop,pdpconcurrent) SELECT 1200, date_trunc('min',fecha - make_interval(0,0,0,0,0, date_part('min',fecha)::int%%20)) AS t2, apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic, sum(datavolumegprsuplink),sum(datavolumegprsdownlink),sum(qoSChange),sum(tariffTime),sum(recordClosure),sum(cGI_SAICHange),sum(rAIChange),sum(dT_Establishment),sum(dT_Removal),sum(eCGIChange),sum(tAIChange),sum(userLocationChange),sum(pdpStart),sum(pdpNormalStop),sum(pdpAbnormalStop),(sum(pdpconcurrent)/20) AS pdpconcurrent FROM (%s) AS selx GROUP BY t2,apnni,gwaddr,gwmcc,gwmnc,snaddr,sntype,snmcc,snmnc,rattype,mcc,mnc,lac,ci,cc,dynamicaddressflag,apnselectionmode,chchselectionmode,imssignalingcontext,withtraffic ; DROP TABLE %s; DELETE FROM cdr_tables WHERE (name) IN (SELECT regexp_split_to_table('%s',E',')); UPDATE cdr_tables SET status=1,ctime=now() WHERE name='%s'; ",sec1200,fromStr,tableStr,tableStr,sec1200);
	retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
	if( !retCode)
	{
	  time(&t2);
	  snprintf(logStr,sizeof(logStr),"RESUME/DROP TABLE %s to %s, %u/%u, %ld sec",tableStr,sec1200,i+1,retInt,t2-t1);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"function:%s;Line:%d;Msg:%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
  }
  PQclear( res);
  if(!retCode)
  {
    if((retInt-i)>0)
      retCode = 1;
  }
  return retCode;
}
/*Create SQL tables for tracking*/
int addSubstrkTable(struct _pgCon *pgCon, char *tableName, char *from, char *to)
{
  int retCode=0;
  char sql[10000];
  char auxStr[10000];
  char logStr[10000];
  PGresult *res=NULL;
  unsigned int i=0, retInt=0;
  snprintf(sql,sizeof(sql),"\
    INSERT INTO cdr.tables(name,ttype)\
      SELECT '%s','substrk'\
      WHERE '%s' NOT IN (SELECT name FROM cdr.tables)\
    RETURNING name,ttype\
  ",tableName,tableName);
  res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
  if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
  {
    retInt = PQntuples(res);
    for(i=0;i<retInt;i++)
    {
      snprintf(sql,sizeof(sql),"\
	CREATE TABLE IF NOT EXISTS %s(CHECK(tor>='%s' AND tor<'%s')) INHERITS(cdr.substrk);\
	CREATE INDEX ON %s(tor,isdn);\
	",tableName,from,to,tableName);
      if(myDB_PQexec(pgCon,sql,auxStr,sizeof(auxStr)))
      {
	snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
    retCode = -1;
      }
      else
      {
	snprintf(logStr,sizeof(logStr),"CREATE TABLE %s",tableName);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  else
  {
	snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  PQclear( res);
  return retCode;
}
int maintainStrkTables(struct _pgCon *pgCon)
{
  int retCode=0;
  int retInt=0, retInt2=0;;
  unsigned int i=0, j=0;
  char sql[10240];
  char logStr[10240];
  char auxStr[10240];
  PGresult *res=NULL, *res2=NULL;
  char *crtime=NULL;
  char fromStr[50000];
  unsigned int fromLen=0,fromCount=0;
  char tableStr[10000];
  unsigned int tableLen=0,tableCount=0;
  time_t t1, t2;
  time_t t3, t4;
  time(&t3);
  do
  {
    /*Delete oldest tables*/
    snprintf(sql,sizeof(sql),"SELECT name FROM cdr.tables WHERE ttype='substrk' AND maxtor<(CURRENT_TIMESTAMP-interval '%ld seconds') AND status=1 ORDER BY ctime ASC",cdrDecParam.strkSqlLen);
    res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
    if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
    {
      retInt = PQntuples(res);
      if(retInt)
      {
	for(i=0;i<retInt;i++)
	{
	  snprintf(sql,sizeof(sql),"DROP TABLE %s; DELETE FROM cdr.tables WHERE name='%s'",PQgetvalue(res,i,0),PQgetvalue(res,i,0));
		  retCode = myDB_PQexec(pgCon, sql, auxStr, sizeof(auxStr));
		  if(!retCode)
		  {
		    snprintf(logStr,sizeof(logStr),"DROP TABLE %s",PQgetvalue(res,i,0));
		    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  }
		  else
		  {
		    snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  }
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear( res);
    /*Compact new data*/
    time(&t1);
    strncpy(sql,"SELECT name,date_trunc('min', ctime) as crtime FROM cdr.tables WHERE status=1 AND ttype='substrk_tmp' GROUP BY name,crtime ORDER BY crtime DESC",sizeof(sql));
    res = myDB_getSQL( pgCon, sql, auxStr, sizeof(auxStr));
    if( (PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_SINGLE_TUPLE))
    {
      retInt = PQntuples(res);
      if(retInt)
      {
	for(i=0,j=0;i<retInt;i++,j++)
	{
	  /* Concatenate FROM clause*/
	  if(i==0)
	  {
	    crtime = PQgetvalue(res,i,1);
	    fromLen = sizeof(fromStr);
	    fromCount = snprintf(fromStr,fromLen," SELECT tor,tablename,isdn,apnni,rattype,mcc,mnc,lac,ci FROM %s ",PQgetvalue(res,i,0));
	    tableLen = sizeof(tableStr);
	    tableCount = snprintf(tableStr,tableLen,"%s",PQgetvalue(res,i,0));
	  }
	  else
	  {
	    if(strcmp(crtime,PQgetvalue(res,i,1)))
	    {
	      break;
	    }
	    fromCount += snprintf(fromStr+fromCount,fromLen-fromCount," UNION ALL SELECT tor,tablename,isdn,apnni,rattype,mcc,mnc,lac,ci FROM %s ",PQgetvalue(res,i,0));
	    tableCount += snprintf(tableStr+tableCount,tableLen-tableCount,",%s",PQgetvalue(res,i,0));
	  }
	}
	snprintf(sql,sizeof(sql),"select date_trunc('hour',tor) as hour, tablename, date_trunc('hour',tor)+interval '1 hour' FROM (%s) AS selx group by hour,tablename;",fromStr);
	res2 = myDB_getSQL(pgCon,sql,auxStr,sizeof(auxStr));
	if( (PQresultStatus(res2) == PGRES_TUPLES_OK) || (PQresultStatus(res2) == PGRES_SINGLE_TUPLE))
	{
	  retInt2 = PQntuples(res2);
	  /* Create tables */
	  for(i=0;(i<retInt2)&&!retCode;i++)
	  {
	    retCode = addSubstrkTable(pgCon,PQgetvalue(res2,i,1),PQgetvalue(res2,i,0),PQgetvalue(res2,i,2));
	  }
	  /* INSERT data*/
	  if(!retCode)
	  {
	    retCode = myDB_PQexec(pgCon, "BEGIN", auxStr, sizeof(auxStr));
	    if(!retCode)
	    {
	      for(i=0;i<retInt2;i++)
	      {
		snprintf(sql,sizeof(sql),"\
		  INSERT INTO %s(tor,isdn,apnni,rattype,mcc,mnc,lac,ci)\
		    SELECT tor,isdn,apnni,rattype,mcc,mnc,lac,ci\
		    FROM (%s) AS selx WHERE tablename='%s'\
		",PQgetvalue(res2,i,1),fromStr,PQgetvalue(res2,i,1));
		retCode = myDB_PQexec(pgCon, sql, auxStr, sizeof(auxStr));
		if(retCode)
		{
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		  break;
		}
	      }
	      if(!retCode)
	      {
		snprintf(sql,sizeof(sql),"DROP TABLE %s; DELETE FROM cdr.tables WHERE name IN (SELECT regexp_split_to_table('%s',E','))",tableStr,tableStr);
		retCode = myDB_PQexec(pgCon, sql, auxStr, sizeof(auxStr));
		if(!retCode)
		{
		  time(&t2);
		  snprintf(logStr,sizeof(logStr),"DROP TABLE %s, %u/%u, %ld sec",tableStr,j,retInt,t2-t1);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
		else
		{
		  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
		  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
		}
	      }
	    }
	    else
	    {
	      snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    }
	    myDB_PQexec(pgCon, "COMMIT", NULL, 0);
	  }
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, sql);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  retCode = -1;
	}
	PQclear( res2);
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s;%d;%s",__FUNCTION__,__LINE__,auxStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear( res);
  }while((retInt-j)&&!retCode&&((time(&t4)-t3)<3600)&&cdrDecParam.maintainSubstrk);
  return retCode;
}
/*
	Compact SQL data process
*/
unsigned int compactPdpTableNum=1000000;
void *cdrCompact(void *p)
{
  int retCode=0;
  struct _cdrCompactArg *cCAP=p;
  char sql[10240];
  char logStr[10240];
  char auxStr[10240];
  int retInt=0;
  PGresult *res=NULL/*, *res2=NULL*/;
  struct _localFlags {
    unsigned paused:1;
  } localFlags = {0};
  //unsigned int delNumber=1000;
  //struct _myDbFile_res *fRes=NULL;
  //struct _myDbFile_tuple *tuple=NULL;
  //time_t t0=0,t1=0;
  uint8_t statConReset=0, trkConReset=0;
  snprintf(logStr,sizeof(logStr),"Loading CDR SQL compact Service threadId=0x%lX",cCAP->ptid);
  debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
  /*Check SQL tracking tables*/
  //cdrDecParam.decoderOn = 1;
  while( cCAP->run)
  {
    if(!cdrDecParam.decoderOn)
    {
      snprintf(logStr,sizeof(logStr),"pn=%d,compact halt", cCAP->id);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      localFlags.paused=1;
    }
    while(!cdrDecParam.decoderOn)
      sleep(1);
    if(localFlags.paused)
    {
      snprintf(logStr,sizeof(logStr),"pn=%d,compact run", cCAP->id);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      localFlags.paused=0;
    }
    /*SQL health check for PGW stat*/
    if( cdrDecParam.pwgStat && (cCAP->maintainPdpstat || cCAP->maintainLotv || cCAP->maintainLotvMean || cCAP->maintainLotvTmp || 
                                                         cCAP->maintainLosd || cCAP->maintainLosdMean || cCAP->maintainLosdTmp))
    {
      /*Get SQL resource*/
      while( !cCAP->pgCon)
      {
	cCAP->pgCon = myDB_selectPgconByGroup(cdrDecParam.statSqlGroup);
	if(!cCAP->pgCon)
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,no SQL resource available for stat", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  sleep(10);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,SQL resource reserved for stat", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
      /*Check SQL connection*/
      PQreset(cCAP->pgCon->pgCon);
      while( myDB_pgLinkStatus( cCAP->pgCon))
      {
        statConReset=0;
	if( myDB_pgLinkReset( cCAP->pgCon))
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,fail to SQL link pgCon reset", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  sleep(10);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,SQL link pgCon reset", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  break;
	}
      }
      PQsetNoticeReceiver(cCAP->pgCon->pgCon, pqnotifyreceiver,NULL);
      if(!statConReset)
      {
        statConReset=1;
      }
      /*Maintain Process*/
      if(cCAP->maintainLotv)
      {
        retCode = maintainLotvTables(cCAP->pgCon);
	if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainLotvTmp)
      {
        retCode = maintainLotvTmpTables(cCAP->pgCon);
	if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainLotvMean)
      {
        retCode = maintainLotvMeanTables(cCAP->pgCon);
        if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainLosd)
      {
        retCode =  maintainLosdTables(cCAP->pgCon);
	if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainLosdTmp)
      {
        retCode = maintainLosdTmpTables(cCAP->pgCon);
	if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainLosdMean)
      {
        retCode = maintainLosdMeanTables(cCAP->pgCon);
        if(retCode)
	{
	  sleep(1);
	  continue;
	}
      }
      if(cCAP->maintainPdpstat)
      {
	do
	{
	  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT * FROM pdpStat WHERE tst<(CURRENT_TIMESTAMP-interval '24 hours') LIMIT %u), sel2 AS (SELECT tst,apn,rattype,gwaddr,srvgwaddr,SUM(pdpstart) AS pdpstart,SUM(pdpstop) AS pdpstop,SUM(pdpupdate) AS pdpupdate,sum(pdpconcurrent) AS pdpconcurrent FROM sel1 GROUP BY tst,apn,rattype,gwaddr,srvgwaddr ORDER BY tst DESC), sel3 AS (SELECT date_trunc('hour',tst) AS tst1,apn,rattype,gwaddr,srvgwaddr,SUM(pdpstart) AS pdpstart,SUM(pdpstop) AS pdpstop,SUM(pdpupdate) AS pdpupdate,max(pdpconcurrent) AS pdpconcurrent FROM sel2 GROUP BY tst1,apn,rattype,gwaddr,srvgwaddr ORDER BY tst1 DESC), ins1 AS (INSERT INTO pdpStat_week(tst,apn,rattype,gwaddr,srvgwaddr,pdpstart,pdpstop,pdpupdate,pdpconcurrent) SELECT * FROM sel3) DELETE FROM pdpStat WHERE pdpStat.id IN (SELECT id FROM sel1);",compactPdpTableNum);
	  res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
	  retInt = atoi(PQcmdTuples(res));
	  PQclear( res);
	  if( retInt < 0)
	  {
	    snprintf(logStr,sizeof(logStr),"%s:%s",__func__,auxStr);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	    //localFlags.pause = 1;
	    break;
	  }
	  else if( retInt > 0)
	  {
	    snprintf(logStr,sizeof(logStr),"SQL compact pdpStat %u rows deleted",retInt);
	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  }
	}
	while( retInt > 0);
      }
    }
    /*SQL health check for subscriber tracking*/
    if( cdrDecParam.subsTrack && cdrDecParam.subsTrackSql && cCAP->maintainSubsTrk)
    {
      /*Get SQL resource*/
      while( !cCAP->pgConTrk)
      {
	cCAP->pgConTrk = myDB_selectPgconByGroup(cdrDecParam.strkSqlGroup);
	if(!cCAP->pgConTrk )
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,no SQL resource available for subscriber tracking", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  sleep(10);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,SQL resource reserved for tracking", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
      /*Check SQL connection*/
      while( myDB_pgLinkStatus( cCAP->pgConTrk))
      {
        trkConReset=0;
	if( myDB_pgLinkReset( cCAP->pgConTrk))
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,fail to SQL link pgConTrk reset", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  sleep(10);
	}
	else
	{
	  snprintf(logStr,sizeof(logStr),"pn=%d,SQL link pgConTrk reset", cCAP->id);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  break;
	}
      }
      if(!trkConReset)
      {
        trkConReset=1;
	PQsetNoticeReceiver(cCAP->pgConTrk->pgCon, pqnotifyreceiver,NULL);
      }
      /*Maintain Process*/
      if(cdrDecParam.maintainSubstrk)
      {
	if(maintainStrkTables( cCAP->pgConTrk))
	{
	  sleep(1);
	  continue;
	}
      }
      else
      {
         sleep(1);
         continue;
      }
      PQreset(cCAP->pgConTrk->pgCon);
    }
    /*Sleep until some SIGNAL is cached*/
    cCAP->pause = 1;
    retInt = sleep( cCAP->sleep);
    //retInt = sleep( 1);
    //if(!retInt)
    //{
      //snprintf(logStr,sizeof(logStr),"CDR compact process: sleep timeout");
      //debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
    //}
    /*Get time*/
    //time(&t0);
    ///*CDR compact routine*/
    ///*Compact LOTV ROWS*/
    ///*Compact LOTV DAY ROWS*/
    //do
    //{
    //  snprintf(sql,sizeof(sql),"WITH tmp AS (SELECT id from lotv_day WHERE fecha<=(CURRENT_TIMESTAMP - interval '24 hours') ORDER BY fecha DESC LIMIT %u) DELETE FROM lotv_day WHERE id IN (SELECT id FROM tmp);",delNumber);
    //  res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
    //  retInt = atoi(PQcmdTuples(res));
    //  PQclear( res);
    //  if( retInt < 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"Error:SQL compact:msg %s",auxStr);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //    //localFlags.pause = 1;
    //    cCAP->lotvRows = 0;
    //    break;
    //  }
    //  else if( retInt > 0)
    //  {
    //    if( cCAP->lotvRows < retInt)
    //      cCAP->lotvRows = 0;
    //    else
    //      cCAP->lotvRows -= retInt;
    //    snprintf(logStr,sizeof(logStr),"SQL compact lotv_day %u rows deleted",retInt);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  }
    //  else
    //  {
    //    cCAP->lotvRows = 0;
    //  }
    //}
    //while( retInt > 0);
    ///*Compact LOTV WEEK ROWS*/
    //do
    //{
    //  snprintf(sql,sizeof(sql),"with f AS (select id from lotv_week where fecha<(CURRENT_TIMESTAMP - interval '7 days') ORDER BY fecha DESC LIMIT %u) DELETE FROM lotv_week WHERE id IN(SELECT id FROM f);",delNumber);
    //  res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
    //  retInt = atoi(PQcmdTuples(res));
    //  PQclear( res);
    //  if( retInt < 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"Error:SQL compact:msg %s",auxStr);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //    //localFlags.pause = 1;
    //    break;
    //  }
    //  else if( retInt > 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"SQL compact lotv_week %u rows deleted",retInt);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  }
    //}
    //while( retInt > 0);
    /*Hourly maintenance*/
    //if( (t0-t1)>=3600)
    //{
    //  t1=t0;
    //  /*Compact PDP tables*/
    //  if(cCAP->maintainPdpstat)
    //  {
    //    do
    //    {
    //      snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT * FROM pdpStat WHERE tst<(CURRENT_TIMESTAMP-interval '24 hours') LIMIT %u), sel2 AS (SELECT tst,apn,rattype,gwaddr,srvgwaddr,SUM(pdpstart) AS pdpstart,SUM(pdpstop) AS pdpstop,SUM(pdpupdate) AS pdpupdate,sum(pdpconcurrent) AS pdpconcurrent FROM sel1 GROUP BY tst,apn,rattype,gwaddr,srvgwaddr ORDER BY tst DESC), sel3 AS (SELECT date_trunc('hour',tst) AS tst1,apn,rattype,gwaddr,srvgwaddr,SUM(pdpstart) AS pdpstart,SUM(pdpstop) AS pdpstop,SUM(pdpupdate) AS pdpupdate,max(pdpconcurrent) AS pdpconcurrent FROM sel2 GROUP BY tst1,apn,rattype,gwaddr,srvgwaddr ORDER BY tst1 DESC), ins1 AS (INSERT INTO pdpStat_week(tst,apn,rattype,gwaddr,srvgwaddr,pdpstart,pdpstop,pdpupdate,pdpconcurrent) SELECT * FROM sel3) DELETE FROM pdpStat WHERE pdpStat.id IN (SELECT id FROM sel1);",compactPdpTableNum);
    //      res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
    //      retInt = atoi(PQcmdTuples(res));
    //      PQclear( res);
    //      if( retInt < 0)
    //      {
    //        snprintf(logStr,sizeof(logStr),"%s:%s",__func__,auxStr);
    //        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //        //localFlags.pause = 1;
    //        break;
    //      }
    //      else if( retInt > 0)
    //      {
    //        snprintf(logStr,sizeof(logStr),"SQL compact pdpStat %u rows deleted",retInt);
    //        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //      }
    //    }
    //    while( retInt > 0);
    //  }
    //}
    ///*COmpact LOSD ROWS*/
    ///*COmpact LOSD DAY ROWS*/
    //do
    //{
    //  snprintf(sql,sizeof(sql),"WITH tmp AS (SELECT id from losd_day WHERE fecha<=(CURRENT_TIMESTAMP - interval '24 hours') ORDER BY fecha DESC LIMIT %u) DELETE FROM losd_day WHERE id IN (SELECT id FROM tmp);",delNumber);
    //  res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
    //  retInt = atoi(PQcmdTuples(res));
    //  PQclear( res);
    //  if( retInt < 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"Error:SQL losd_day compact:msg %s",auxStr);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //    //localFlags.pause = 1;
    //    cCAP->losdRows = 0;
    //    break;
    //  }
    //  else if( retInt > 0)
    //  {
    //    if( cCAP->losdRows < retInt)
    //      cCAP->losdRows = 0;
    //    else
    //      cCAP->losdRows -= retInt;
    //    snprintf(logStr,sizeof(logStr),"SQL compact losd_day %u rows deleted",retInt);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  }
    //  else
    //  {
    //    cCAP->losdRows = 0;
    //  }
    //}
    //while( retInt > 0);
    ///*COmpact LOSD WEEK ROWS*/
    //do
    //{
    //  snprintf(sql,sizeof(sql),"with f AS (select id from losd_week where fecha<(CURRENT_TIMESTAMP - interval '7 days') ORDER BY fecha DESC LIMIT %u) DELETE FROM losd_week WHERE id IN(SELECT id FROM f);",delNumber);
    //  res = myDB_getSQL( cCAP->pgCon, sql, auxStr, sizeof(auxStr));
    //  retInt = atoi(PQcmdTuples(res));
    //  PQclear( res);
    //  if( retInt < 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"Error:SQL losd_week compact:msg %s",auxStr);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //    //localFlags.pause = 1;
    //    break;
    //  }
    //  else if( retInt > 0)
    //  {
    //    snprintf(logStr,sizeof(logStr),"SQL compact losd_week %u rows deleted",retInt);
    //    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  }
    //}
    //while( retInt > 0);
    ///*Delete oldest tables for NON SQL tracking*/
    //fRes = myDbFile_listTables(cdrDb_id);
    //if( fRes)
    //{
    //  for(tuple=fRes->objs;tuple;tuple=tuple->next)
    //  {
    //    #define SUBSTRACK_TABLE_PREFIX "substrack_"
    //    if( !strncmp(tuple->obj,SUBSTRACK_TABLE_PREFIX,strlen(SUBSTRACK_TABLE_PREFIX)))
    //    {
    //      time_t t;
    //      struct tm tM;
    //      char tableName[128];
    //      int retCode;
    //      time(&t);
    //      //t = t-(30*24*60*60);
    //      //t = t-substrackLen;
    //      t = t-cdrDecParam.strkFileLen;
    //      localtime_r(&t,&tM);
    //      strcpy(tableName,SUBSTRACK_TABLE_PREFIX);
    //      strftime(tableName+strlen(SUBSTRACK_TABLE_PREFIX),sizeof(tableName),"%Y%m%d",&tM);
    //      if( strcmp(tableName,tuple->obj)>0)
    //      {
    //        snprintf(logStr,sizeof(logStr),"Deleting table %s, %s",(char *)tuple->obj,tableName);
    //        debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //        retCode = myDbFile_dropTable( tuple->obj);
    //        if( retCode<0)
    //        {
    //          snprintf(logStr,sizeof(logStr),"Error:%d, myDbFile_dropTable",retCode);
    //          debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_,logStr);
    //        }
    //      }
    //    }
    //  }
    //}
    //myDbFile_resClear(fRes);
  }
  snprintf(logStr,sizeof(logStr),"Ending CDR SQL compact Service threadId=0x%lX",cCAP->ptid);
  debugLogging(DEBUG_CRITICAL,"cdrDecoder",logStr);
  return NULL;
}
/*
  Temporary table for PGWCDR combination
*/
int pgwcdrcmb_CreateTmpTable( struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char logStr[512];
  char auxStr[512];
  char sql[3072];
  snprintf(sql,sizeof(sql)," \
    CREATE TEMPORARY TABLE %s_pgwcdrcmb( \
      id bigint primary key, \
      pgwcdrcmbid bigint,\
      lastupdate TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(), \
      pgwid int, \
      sgwid int, \
      apnid int, \
      dynamicAddressFlag boolean default true, \
      apnSelectionMode smallint, \
      cc int NOT NULL, \
      chChSelectionMode smallint, \
      iMSsignalingContext boolean default false, \
      cid int \
      ) /*ON COMMIT DELETE ROWS*/; \
  ",tableName);
  retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
  if( retCode)
  {
    retCode = -2;
    snprintf(logStr,sizeof(logStr),"Error:DB creating TEMPORARY TABLE %s,retCode=%d,%s",tableName,retCode,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return retCode;
}
/*
  Temporary table for insert CI data
*/
int cdrCiCreateTmpTable( struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char logStr[512];
  char auxStr[512];
  char sql[3072];
  snprintf(sql,sizeof(sql),"CREATE TEMPORARY TABLE %s_ci( id bigint PRIMARY KEY, rattype smallint, mcc smallint, mnc smallint, lac int, ci int) /*ON COMMIT DELETE ROWS*/; ",tableName);
  retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
  if( retCode)
  {
    retCode = -2;
    snprintf(logStr,sizeof(logStr),"Error:DB creating TEMPORARY TABLE %s,retCode=%d,%s",tableName,retCode,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return retCode;
}
/*
	Create a new temporary table fo LOTV stats
		return:
			0= success
			-1= no resource available
			-2= DB error
			-3= invalid argument
*/
int cdrPGWCoccCreateTmpTable( struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char logStr[512];
  char auxStr[512];
  char sql[13072];
  snprintf(sql, sizeof(sql),\
    "CREATE TEMPORARY TABLE %s_lotv( fecha timestamp without time zone,\
     pgwcdrcmbid int,\
     withtraffic boolean,\
     datavolumegprsuplink bigint,\
     datavolumegprsdownlink bigint,\
     qoSChange bigint default 0,\
     tariffTime bigint default 0,\
     recordClosure bigint default 0,\
     cGI_SAICHange bigint default 0,\
     rAIChange bigint default 0,\
     dT_Establishment bigint default 0,\
     dT_Removal bigint default 0,\
     eCGIChange bigint default 0,\
     tAIChange bigint default 0,\
     userLocationChange bigint default 0,\
     pdpStart bigint default 0,\
     pdpNormalStop bigint default 0,\
     pdpAbnormalStop bigint default 0,\
     pdpconcurrent real default 0) ON COMMIT DELETE ROWS; ",\
    tableName);
  retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
  if( retCode)
  {
    retCode = -2;
    snprintf(logStr,sizeof(logStr),"Error:DB creating TEMPORARY TABLE %s,retCode=%d,%s",tableName,retCode,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return retCode;
}
/*
	Create a new temporary table fo LOSD stats
		return:
			0= success
			-1= no resource available
			-2= DB error
			-3= invalid argument
*/
int cdrPGWCoscCreateTmpTable( struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char logStr[512];
  char auxStr[512];
  char sql[13072];
  /*Select an idle connection*/
  snprintf(sql,sizeof(sql)," \
    CREATE TEMPORARY TABLE %s_losd( \
      fecha timestamp without time zone, \
      apnni varchar,\
      rattype smallint,\
      gwaddr inet,\
      gwmcc smallint,\
      gwmnc smallint,\
      snaddr inet,\
      sntype smallint,\
      snmcc smallint,\
      snmnc smallint,\
      cc int,\
      crbn varchar,\
      datavolumefbcuplink bigint default 0, \
      datavolumefbcdownlink bigint default 0, \
      sessions real default 0, \
      hits bigint default 0, \
      qoSChange bigint default 0, \
      sGSNChange bigint default 0, \
      sGSNPLMNIDChange bigint default 0, \
      tariffTimeSwitch bigint default 0, \
      pDPContextRelease bigint default 0, \
      rATChange bigint default 0, \
      serviceIdledOut bigint default 0, \
      configurationChange bigint default 0, \
      serviceStop bigint default 0, \
      dCCATimeThresholdReached bigint default 0, \
      dCCAVolumeThresholdReached bigint default 0, \
      dCCAServiceSpecificUnitThresholdReached bigint default 0, \
      dCCATimeExhausted bigint default 0, \
      dCCAVolumeExhausted bigint default 0, \
      dCCAValidityTimeout bigint default 0, \
      dCCAReauthorisationRequest bigint default 0, \
      dCCAContinueOngoingSession bigint default 0, \
      dCCARetryAndTerminateOngoingSession bigint default 0, \
      dCCATerminateOngoingSession bigint default 0, \
      cGI_SAIChange bigint default 0, \
      rAIChange bigint default 0, \
      dCCAServiceSpecificUnitExhausted bigint default 0, \
      recordClosure bigint default 0, \
      timeLimit bigint default 0, \
      volumeLimit bigint default 0, \
      serviceSpecificUnitLimit bigint default 0, \
      envelopeClosure bigint default 0, \
      eCGIChange bigint default 0, \
      tAIChange bigint default 0, \
      userLocationChange bigint default 0, \
      pgwcdrcmbid bigint,\
      ratinggroup int,\
      serviceidentifier int, \
      resultcode int, \
      failurehandlingcontinue boolean\
      ) ON COMMIT DELETE ROWS;",tableName);
  retCode = myDB_PQexec( pgCon, sql, auxStr, sizeof(auxStr));
  /*DB exec error*/
  if( retCode)
  {
    retCode = -2;
    snprintf(logStr,sizeof(logStr),"Error:%s,%s",__FUNCTION__,auxStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  return retCode;
}
/*
	LOTV stats to SQL in binay mode (exec prepared)
*/
int cdrPGWCoccStatInsertSQL( void *coccIt, struct _pgCon *pgCon, char *tableName, void *tableIt)
{
  #define COCC_VALUES_LEN 19
  #define COCC_COPY_BUFFER 1024*1024
  char *cocc_buffer=NULL;
  uint32_t buffer_len=0;
  uint32_t cocc_buffer_count=0;
  int retCode=0;
  char sql[11024];
  char logStr[512];
  char msgStr[512];
  struct _pgwRecordCOCC *localPgwRecordCOCC=NULL;
  char timestamp[32];
  char *values[COCC_VALUES_LEN]={NULL};
  int len[COCC_VALUES_LEN]={0};
  int format[COCC_VALUES_LEN]={0};
  unsigned int i=0;
  PGresult *res;
  char gwaddr[64];
  char snaddr[64];
  char tname[64];
    //printf("printf buffer\n");
  snprintf(sql,sizeof(sql),"CREATE TABLE lotv_%s (LIKE lotv_tpl INCLUDING INDEXES); INSERT INTO cdr_tables (name,ttype) VALUES ('lotv_%s','lotv_tmp')",tableName,tableName);
  retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr)); 
  if( retCode)
  {
    retCode = -1;
    snprintf(logStr,sizeof(logStr),"Error cdrPGWCoscStatInsertSQL:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  cocc_buffer = malloc(COCC_COPY_BUFFER);
  buffer_len = COCC_COPY_BUFFER;
  while( (localPgwRecordCOCC = myDB_getNextRow( coccIt)))
  {
    if((buffer_len-cocc_buffer_count)<COCC_COPY_BUFFER)
    {
      buffer_len += COCC_COPY_BUFFER;
      cocc_buffer = realloc(cocc_buffer,buffer_len);
    }
    //printf("cocc_buffer_count=%u\n",cocc_buffer_count);
    strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&localPgwRecordCOCC->changeTime);
    inet_ntop(AF_INET6,&localPgwRecordCOCC->gwaddr,gwaddr,sizeof(gwaddr)); 
    inet_ntop(AF_INET6,&localPgwRecordCOCC->snaddr,snaddr,sizeof(snaddr)); 
    strftime(tname,sizeof(tname),"cdr_lotv_60_%Y%m%d%H%M",&localPgwRecordCOCC->changeTime);
    cocc_buffer_count += snprintf(cocc_buffer+cocc_buffer_count,buffer_len-cocc_buffer_count,"'%s',%s,%s,%s,%u,%u,%s,%u,%u,%u,%u,%u,%u,%u,%u,%u,%s,%u,%u,%s,%s,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%f\n",\
      timestamp,\
      tname,\
      //  apnni varchar,
      localPgwRecordCOCC->apnni,\
      //  gwaddr inet,
      gwaddr,\
      //  gwmcc smallint,
      localPgwRecordCOCC->gwmcc,\
      //  gwmnc smallint,
      localPgwRecordCOCC->gwmnc,\
      //  snaddr inet,
      snaddr,\
      //  sntype smallint,
      localPgwRecordCOCC->sntype,\
      //  snmcc smallint,
      localPgwRecordCOCC->snmcc,\
      //  snmnc smallint,
      localPgwRecordCOCC->snmnc,\
      //  rattype smallint,
      localPgwRecordCOCC->rATType,\
      //  mcc smallint,
      localPgwRecordCOCC->uLI.mcc,\
      //  mnc smallint,
      localPgwRecordCOCC->uLI.mnc,\
      //  lac int,
      localPgwRecordCOCC->uLI.lac,\
      //  ci int,
      localPgwRecordCOCC->uLI.uliType.ecgi ? localPgwRecordCOCC->uLI.eci: localPgwRecordCOCC->uLI.ci,\
      //  cc int,
      localPgwRecordCOCC->cc,\
      //  dynamicaddressflag boolean,
      localPgwRecordCOCC->dynamicaddressflag ? "t":"f",\
      //  apnselectionmode smallint,
      localPgwRecordCOCC->apnselectionmode,\
      //  chchselectionmode smallint,
      localPgwRecordCOCC->chchselectionmode,\
      //  imssignalingcontext boolean,
      localPgwRecordCOCC->imssignalingcontext ? "t":"f",\
      localPgwRecordCOCC->withtraffic ? "t":"f",\
      localPgwRecordCOCC->dataVolumeGPRSUplink,\
      localPgwRecordCOCC->dataVolumeGPRSDownlink,\
      localPgwRecordCOCC->qoSChange,\
      localPgwRecordCOCC->tariffTime,\
      localPgwRecordCOCC->recordClosure,\
      localPgwRecordCOCC->cGI_SAICHange,\
      localPgwRecordCOCC->rAIChange,\
      localPgwRecordCOCC->dT_Establishment,\
      localPgwRecordCOCC->dT_Removal,\
      localPgwRecordCOCC->eCGIChange,\
      localPgwRecordCOCC->tAIChange,\
      localPgwRecordCOCC->userLocationChange,\
      localPgwRecordCOCC->pdpStart,\
      localPgwRecordCOCC->pdpNormalStop,\
      localPgwRecordCOCC->pdpAbnormalStop,\
      localPgwRecordCOCC->pdpconcurrent\
      );
    continue;
    i++;
    /* EXEC PREPARED */
    retCode = myDB_PQexecPrepare( pgCon, tableName, COCC_VALUES_LEN, (const char * const *)values, (const int *)len, (const int *)format, msgStr, sizeof(msgStr));
    if( retCode)
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"Error PGWCoccStatInsertSQL:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      break;
    }
  }
  if( !retCode)
  {
    snprintf(sql,sizeof(sql),"\
      COPY lotv_%s(fecha,\
        tname,\
        apnni,\
	gwaddr,\
	gwmcc,\
	gwmnc,\
	snaddr,\
	sntype,\
	snmcc,\
	snmnc,\
	rattype,\
	mcc,\
	mnc,\
	lac,\
	ci,\
	cc,\
	dynamicaddressflag,\
	apnselectionmode,\
	chchselectionmode,\
	imssignalingcontext,\
	withtraffic,\
	dataVolumeGPRSUplink,\
	dataVolumeGPRSDownlink,\
	qoSChange,\
	tariffTime,\
	recordClosure,\
	cGI_SAICHange,\
	rAIChange,\
	dT_Establishment,\
	dT_Removal,\
	eCGIChange,\
	tAIChange,\
	userLocationChange,\
	pdpStart,\
	pdpNormalStop,\
	pdpAbnormalStop,\
	pdpconcurrent)\
	FROM STDIN WITH (FORMAT csv)\
      ;"\
      ,tableName);
      //strncpy(logStr,"Start COPY LOTV",sizeof(logStr));
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res) == PGRES_COPY_IN)
    {
      PQclear(res);
      if(PQputCopyData(pgCon->pgCon,cocc_buffer,cocc_buffer_count)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
    }
    res = PQgetResult(pgCon->pgCon);
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear(res);
    if(!retCode)
    {
      snprintf(sql,sizeof(sql),"UPDATE cdr_tables SET status=1 WHERE name='lotv_%s'",tableName);
      retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr)); 
      if( retCode)
      {
	retCode = -1;
	snprintf(logStr,sizeof(logStr),"%s;%d,Result=%d,%s",__FUNCTION__,__LINE__,retCode,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  if(cocc_buffer) free(cocc_buffer);
  return retCode;
}
/*
	LOSD stats to SQL in binay mode (exec prepared)
*/
int cdrPGWCoscStatInsertSQL( void *coscIt, struct _pgCon *pgCon, char *tableName, void *tableIt)
{
  #define COSC_VALUES_LEN 40
  #define COSC_COPY_BUFFER 1024*1024
  unsigned int buffer_len = COSC_COPY_BUFFER;
  char *buffer=NULL;
  uint32_t buffer_count=0;
  int retCode=0;
  char sql[10024];
  char logStr[1024];
  char msgStr[1024];
  struct _pgwRecordCOSC *localPgwRecordCOSC=NULL;
  char timestamp[32];
  PGresult *res;
  char gwaddr[64];
  char snaddr[64];
  char tname[64];
  buffer = malloc(buffer_len);
  if(buffer==NULL)
  {
    return -1;
  }
  snprintf(sql,sizeof(sql),"CREATE TABLE losd_%s (LIKE losd_tpl INCLUDING INDEXES); INSERT INTO cdr_tables (name,ttype) VALUES ('losd_%s','losd_tmp')",tableName,tableName);
  retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr)); 
  if( retCode)
  {
    retCode = -1;
    snprintf(logStr,sizeof(logStr),"Error cdrPGWCoscStatInsertSQL:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  }
  while( (localPgwRecordCOSC = myDB_getNextRow( coscIt)))
  {
    strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&localPgwRecordCOSC->timeOfReport);
    if((buffer_len - buffer_count) < 1000)
    {
      buffer = realloc(buffer,buffer_len+COSC_COPY_BUFFER);
      if(buffer)
      {
        buffer_len+=COSC_COPY_BUFFER;
      }
    }
    inet_ntop(AF_INET6,&localPgwRecordCOSC->gwaddr,gwaddr,sizeof(gwaddr)); 
    inet_ntop(AF_INET6,&localPgwRecordCOSC->snaddr,snaddr,sizeof(snaddr)); 
    strftime(tname,sizeof(tname),"cdr_losd_60_%Y%m%d%H%M",&localPgwRecordCOSC->timeOfReport);
    buffer_count += snprintf(buffer+buffer_count,buffer_len-buffer_count,\
      "%s,%s,%u,%u,%u,%u,%s,%u,%u,%s,%u,%s,'%s',%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%f,%lu,%u,%u,%u,%s\n",\
      //tname
      tname,\
      //crbn
      localPgwRecordCOSC->crbn,\
      //cc
      localPgwRecordCOSC->cc,\
      //snmnc
      localPgwRecordCOSC->snmnc,\
      //snmcc
      localPgwRecordCOSC->snmcc,\
      //sntype
      localPgwRecordCOSC->sntype,\
      //snaddr
      snaddr,\
      //gwmnc
      localPgwRecordCOSC->gwmnc,\
      //gwmcc
      localPgwRecordCOSC->gwmcc,\
      //gwaddr
      gwaddr,\
      //rattype
      localPgwRecordCOSC->rATType,\
      //APN
      localPgwRecordCOSC->apnni,\
      //fecha
      timestamp,\
      //datavolumefbcuplink
      localPgwRecordCOSC->datavolumeFBCUplink,\
      //datavolumefbcdownlink
      localPgwRecordCOSC->datavolumeFBCDownlink,\
      //qoSChange
      localPgwRecordCOSC->qoSChange,\
      //sGSNChange
      localPgwRecordCOSC->sGSNChange,\
      //sGSNPLMNIDChange
      localPgwRecordCOSC->sGSNPLMNIDChange,\
      //tariffTimeSwitch
      localPgwRecordCOSC->tariffTimeSwitch,\
      //pDPContextRelease
      localPgwRecordCOSC->pDPContextRelease,\
      //rATChange
      localPgwRecordCOSC->rATChange,\
      //serviceIdledOut
      localPgwRecordCOSC->serviceIdledOut,\
      //configurationChange
      localPgwRecordCOSC->configurationChange,\
      //serviceStop
      localPgwRecordCOSC->serviceStop,\
      //dCCATimeThresholdReached
      localPgwRecordCOSC->dCCATimeThresholdReached,\
      //dCCAVolumeThresholdReached
      localPgwRecordCOSC->dCCAVolumeThresholdReached,\
      //dCCAServiceSpecificUnitThresholdReached
      localPgwRecordCOSC->dCCAServiceSpecificUnitThresholdReached,\
      //dCCATimeExhausted
      localPgwRecordCOSC->dCCATimeExhausted,\
      //dCCAVolumeExhausted
      localPgwRecordCOSC->dCCAVolumeExhausted,\
      //dCCAValidityTimeout
      localPgwRecordCOSC->dCCAValidityTimeout,\
      //dCCAReauthorisationRequest
      localPgwRecordCOSC->dCCAReauthorisationRequest,\
      //dCCAContinueOngoingSession
      localPgwRecordCOSC->dCCAContinueOngoingSession,\
      //dCCARetryAndTerminateOngoingSession
      localPgwRecordCOSC->dCCARetryAndTerminateOngoingSession,\
      //dCCATerminateOngoingSession
      localPgwRecordCOSC->dCCATerminateOngoingSession,\
      //cGI_SAIChange
      localPgwRecordCOSC->cGI_SAIChange,\
      //rAIChange
      localPgwRecordCOSC->rAIChange,\
      //dCCAServiceSpecificUnitExhausted
      localPgwRecordCOSC->dCCAServiceSpecificUnitExhausted,\
      //recordClosure
      localPgwRecordCOSC->recordClosure,\
      //timeLimit
      localPgwRecordCOSC->timeLimit,\
      //volumeLimit
      localPgwRecordCOSC->volumeLimit,\
      //serviceSpecificUnitLimit
      localPgwRecordCOSC->serviceSpecificUnitLimit,\
      //envelopeClosure
      localPgwRecordCOSC->envelopeClosure,\
      //eCGIChange
      localPgwRecordCOSC->eCGIChange,\
      //tAIChange
      localPgwRecordCOSC->tAIChange,\
      //userLocationChange
      localPgwRecordCOSC->userLocationChange,\
      //sessions
      localPgwRecordCOSC->sessions,\
       //hits
      localPgwRecordCOSC->hits,\
      //ratinggroup,
      localPgwRecordCOSC->ratingGroup,\
      //serviceidentifier
      localPgwRecordCOSC->serviceIdentifier,\
      //resultcode
      localPgwRecordCOSC->resultCode,\
      //failurehandlingcontinue 
      localPgwRecordCOSC->failureHandlingContinue ? "t":"f"\
      );
    //serviceIdledOut = htobe64(localPgwRecordCOSC->serviceIdledOut);
    //rATChange = htobe64(localPgwRecordCOSC->rATChange);
    //qoSChange = htobe64(localPgwRecordCOSC->qoSChange);
    //sGSNChange = htobe64(localPgwRecordCOSC->sGSNChange);
    //sGSNPLMNIDChange = htobe64(localPgwRecordCOSC->sGSNPLMNIDChange);
    //tariffTimeSwitch = htobe64(localPgwRecordCOSC->tariffTimeSwitch);
    //pDPContextRelease = htobe64(localPgwRecordCOSC->pDPContextRelease);
    //dCCATimeExhausted = htobe64(localPgwRecordCOSC->dCCATimeExhausted);
    //eCGIChange = htobe64(localPgwRecordCOSC->eCGIChange);
    //envelopeClosure = htobe64(localPgwRecordCOSC->envelopeClosure);
    //serviceSpecificUnitLimit = htobe64(localPgwRecordCOSC->serviceSpecificUnitLimit);
    //volumeLimit = htobe64(localPgwRecordCOSC->volumeLimit);
    //timeLimit = htobe64(localPgwRecordCOSC->timeLimit);
    //recordClosure = htobe64(localPgwRecordCOSC->recordClosure);
    //dCCAServiceSpecificUnitExhausted = htobe64(localPgwRecordCOSC->dCCAServiceSpecificUnitExhausted);
    //dCCATerminateOngoingSession = htobe64(localPgwRecordCOSC->dCCATerminateOngoingSession);
    //dCCARetryAndTerminateOngoingSession = htobe64(localPgwRecordCOSC->dCCARetryAndTerminateOngoingSession);
    //dCCAContinueOngoingSession = htobe64(localPgwRecordCOSC->dCCAContinueOngoingSession);
    //dCCAReauthorisationRequest = htobe64(localPgwRecordCOSC->dCCAReauthorisationRequest);
    //dCCAValidityTimeout = htobe64(localPgwRecordCOSC->dCCAValidityTimeout);
    //dCCAVolumeExhausted = htobe64(localPgwRecordCOSC->dCCAVolumeExhausted);
    //pgwcdrcmbid = htobe64(localPgwRecordCOSC->pgwcdrcmbid);
    //hits = htobe64(localPgwRecordCOSC->hits);
    //snprintf(sessions,sizeof(sessions),"%f",localPgwRecordCOSC->sessions);
    //tAIChange = htobe64(localPgwRecordCOSC->tAIChange);
    //userLocationChange = htobe64(localPgwRecordCOSC->userLocationChange);
    //dCCAServiceSpecificUnitThresholdReached = htobe64(localPgwRecordCOSC->dCCAServiceSpecificUnitThresholdReached);
    //cGI_SAIChange = htobe64(localPgwRecordCOSC->cGI_SAIChange);
    //rAIChange = htobe64(localPgwRecordCOSC->rAIChange);
    //serviceStop = htobe64(localPgwRecordCOSC->serviceStop);
    ///* serviceidentifier*/
    //values[37] = (char *)&localPgwRecordCOSC->serviceIdentifier;
    //len[37] = sizeof(localPgwRecordCOSC->serviceIdentifier);
    //format[37] = 1;
    ///* resultcode*/
    //values[38] = (char *)&localPgwRecordCOSC->resultCode;
    //len[38] = sizeof(localPgwRecordCOSC->resultCode);
    //format[38] = 1;
    ///* tAIChange*/
    //values[31] = (char *)&tAIChange;
    //len[31] = sizeof(tAIChange);
    //format[31] = 1;
    ///* userLocationChange*/
    //values[32] = (char *)&userLocationChange;
    //len[32] = sizeof(userLocationChange);
    //format[32] = 1;
    ///* sessions*/
    //values[33] = (char *)sessions;
    //len[33] = 0;
    //format[33] = 0;
    ///* hits*/
    //values[34] = (char *)&hits;
    //len[34] = sizeof(hits);
    //format[34] = 1;
    ///* pgwcdrcmbid*/
    //values[35] = (char *)&pgwcdrcmbid;
    //len[35] = sizeof(pgwcdrcmbid);
    //format[35] = 1;
    ///* ratinggroup*/
    //values[36] = (char *)&localPgwRecordCOSC->ratingGroup;
    //len[36] = sizeof(localPgwRecordCOSC->ratingGroup);
    //format[36] = 1;
    ///* dCCAServiceSpecificUnitExhausted*/
    //values[24] = (char *)&dCCAServiceSpecificUnitExhausted;
    //len[24] = sizeof(dCCAServiceSpecificUnitExhausted);
    //format[24] = 1;
    ///* recordClosure*/
    //values[25] = (char *)&recordClosure;
    //len[25] = sizeof(recordClosure);
    //format[25] = 1;
    ///* timeLimit*/
    //values[26] = (char *)&timeLimit;
    //len[26] = sizeof(timeLimit);
    //format[26] = 1;
    ///* volumeLimit*/
    //values[27] = (char *)&volumeLimit;
    //len[27] = sizeof(volumeLimit);
    //format[27] = 1;
    ///* serviceSpecificUnitLimit*/
    //values[28] = (char *)&serviceSpecificUnitLimit;
    //len[28] = sizeof(serviceSpecificUnitLimit);
    //format[28] = 1;
    ///* envelopeClosure*/
    //values[29] = (char *)&envelopeClosure;
    //len[29] = sizeof(envelopeClosure);
    //format[29] = 1;
    ///* eCGIChange*/
    //values[30] = (char *)&eCGIChange;
    //len[30] = sizeof(eCGIChange);
    //format[30] = 1;
    ///* failurehandlingcontinue*/
    //values[39] = (char *)&localPgwRecordCOSC->failureHandlingContinue;
    //len[39] = sizeof(localPgwRecordCOSC->failureHandlingContinue);
    //format[39] = 1;
    ///* dCCATimeExhausted*/
    //values[15] = (char *)&dCCATimeExhausted;
    //len[15] = sizeof(dCCATimeExhausted);
    //format[15] = 1;
    ///* dCCAVolumeExhausted*/
    //values[16] = (char *)&dCCAVolumeExhausted;
    //len[16] = sizeof(dCCAVolumeExhausted);
    //format[16] = 1;
    ///* dCCAValidityTimeout*/
    //values[17] = (char *)&dCCAValidityTimeout;
    //len[17] = sizeof(dCCAValidityTimeout);
    //format[17] = 1;
    ///* dCCAReauthorisationRequest*/
    //values[18] = (char *)&dCCAReauthorisationRequest;
    //len[18] = sizeof(dCCAReauthorisationRequest);
    //format[18] = 1;
    ///* dCCAContinueOngoingSession*/
    //values[19] = (char *)&dCCAContinueOngoingSession;
    //len[19] = sizeof(dCCAContinueOngoingSession);
    //format[19] = 1;
    ///* dCCARetryAndTerminateOngoingSession*/
    //values[20] = (char *)&dCCARetryAndTerminateOngoingSession;
    //len[20] = sizeof(dCCARetryAndTerminateOngoingSession);
    //format[20] = 1;
    ///* dCCATerminateOngoingSession*/
    //values[21] = (char *)&dCCATerminateOngoingSession;
    //len[21] = sizeof(dCCATerminateOngoingSession);
    //format[21] = 1;
    ///* cGI_SAIChange*/
    //values[22] = (char *)&cGI_SAIChange;
    //len[22] = sizeof(cGI_SAIChange);
    //format[22] = 1;
    ///* rAIChange*/
    //values[23] = (char *)&rAIChange;
    //len[23] = sizeof(rAIChange);
    //format[23] = 1;
    ///*fecha*/
    //values[0] = timestamp;
    //len[0] = 0;
    //format[0] = 0;
    ///* datavolumefbcuplin*/
    //values[1] = (char *)&localPgwRecordCOSC->datavolumeFBCUplink;
    //len[1] = sizeof(localPgwRecordCOSC->datavolumeFBCUplink);
    //format[1] = 1;
    ///* datavolumefbcdownlink*/
    //values[2] = (char *)&localPgwRecordCOSC->datavolumeFBCDownlink;
    //len[2] = sizeof(localPgwRecordCOSC->datavolumeFBCDownlink);
    //format[2] = 1;
    ///* qoSChange*/
    //values[3] = (char *)&qoSChange;
    //len[3] = sizeof(qoSChange);
    //format[3] = 1;
    ///* sGSNChange*/
    //values[4] = (char *)&sGSNChange;
    //len[4] = sizeof(sGSNChange);
    //format[4] = 1;
    ///* sGSNPLMNIDChange*/
    //values[5] = (char *)&sGSNPLMNIDChange;
    //len[5] = sizeof(sGSNPLMNIDChange);
    //format[5] = 1;
    ///* tariffTimeSwitch*/
    //values[6] = (char *)&tariffTimeSwitch;
    //len[6] = sizeof(tariffTimeSwitch);
    //format[6] = 1;
    ///* pDPContextRelease*/
    //values[7] = (char *)&pDPContextRelease;
    //len[7] = sizeof(pDPContextRelease);
    //format[7] = 1;
    ///* rATChange*/
    //values[8] = (char *)&rATChange;
    //len[8] = sizeof(rATChange);
    //format[8] = 1;
    ///* serviceIdledOut*/
    //values[9] = (char *)&serviceIdledOut;
    //len[9] = sizeof(serviceIdledOut);
    //format[9] = 1;
    ///* configurationChange*/
    //values[10] = (char *)&configurationChange;
    //len[10] = sizeof(configurationChange);
    //format[10] = 1;
    ///* serviceStop*/
    //values[11] = (char *)&serviceStop;
    //len[11] = sizeof(serviceStop);
    //format[11] = 1;
    ///* dCCATimeThresholdReached*/
    //values[12] = (char *)&dCCATimeThresholdReached;
    //len[12] = sizeof(dCCATimeThresholdReached);
    //format[12] = 1;
    ///* dCCAVolumeThresholdReached*/
    //values[13] = (char *)&dCCAVolumeThresholdReached;
    //len[13] = sizeof(dCCAVolumeThresholdReached);
    //format[13] = 1;
    ///* dCCAServiceSpecificUnitThresholdReached*/
    //values[14] = (char *)&dCCAServiceSpecificUnitThresholdReached;
    //len[14] = sizeof(dCCAServiceSpecificUnitThresholdReached);
    //format[14] = 1;
    //retCode = myDB_PQexecPrepare( pgCon, preparedName, COSC_VALUES_LEN, (const char * const *)values, len, format, msgStr, sizeof(msgStr)); 
    //if( retCode)
    //{
    //  retCode = -1;
    //  snprintf(logStr,sizeof(logStr),"Error cdrPGWCoscStatInsertSQL:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    //  break;
    //}
  }
  if( !retCode)
  {
    snprintf(sql,sizeof(sql),\
	"COPY losd_%s( \
	 tname,\
	 crbn,\
	 cc,\
	 snmnc,\
	 snmcc,\
	 sntype,\
	 snaddr,\
	 gwmnc,\
	 gwmcc,\
	 gwaddr,\
	 rattype,\
	 apnni,\
	 fecha,\
	 datavolumefbcuplink,\
	 datavolumefbcdownlink,\
	 qoSChange,\
	 sGSNChange,\
	 sGSNPLMNIDChange,\
	 tariffTimeSwitch,\
	 pDPContextRelease,\
	 rATChange,\
	 serviceIdledOut,\
	 configurationChange,\
	 serviceStop,\
	 dCCATimeThresholdReached,\
	 dCCAVolumeThresholdReached,\
	 dCCAServiceSpecificUnitThresholdReached,\
	 dCCATimeExhausted,\
	 dCCAVolumeExhausted,\
	 dCCAValidityTimeout,\
	 dCCAReauthorisationRequest,\
	 dCCAContinueOngoingSession,\
	 dCCARetryAndTerminateOngoingSession,\
	 dCCATerminateOngoingSession,\
	 cGI_SAIChange,\
	 rAIChange,\
	 dCCAServiceSpecificUnitExhausted,\
	 recordClosure,\
	 timeLimit,\
	 volumeLimit,\
	 serviceSpecificUnitLimit,\
	 envelopeClosure,\
	 eCGIChange,\
	 tAIChange,\
	 userLocationChange,\
	 sessions,\
	 hits,\
	 ratinggroup,\
	 serviceidentifier,\
	 resultcode,\
	 failurehandlingcontinue)\
	 FROM STDIN WITH (FORMAT csv);",\
       tableName);
      //strncpy(logStr,"Start COPY LOSD",sizeof(logStr));
      //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    res = myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
    if(PQresultStatus(res) == PGRES_COPY_IN)
    {
      PQclear(res);
      if(PQputCopyData(pgCon->pgCon,buffer,buffer_count)==-1)
      {
	snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
      else
      {
        if(PQputCopyEnd(pgCon->pgCon,NULL)==-1)
	{
	  snprintf(logStr,sizeof(logStr),"%s,%d,PQputCopyData Error",__FUNCTION__,__LINE__);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	}
      }
    }
    else
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY error,%s",__FUNCTION__,__LINE__,PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      PQclear(res);
    }
    res = PQgetResult(pgCon->pgCon);
    if(PQresultStatus(res)==PGRES_FATAL_ERROR)
    {
      snprintf(logStr,sizeof(logStr),"%s,%d,COPY %s %s",__FUNCTION__,__LINE__,PQresStatus(PQresultStatus(res)),PQresultErrorMessage(res));
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      retCode = -1;
    }
    PQclear(res);
    if(!retCode)
    {
      snprintf(sql,sizeof(sql),"UPDATE cdr_tables SET status=1 WHERE name='losd_%s'",tableName);
      retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr)); 
      if( retCode)
      {
	retCode = -1;
	snprintf(logStr,sizeof(logStr),"Error cdrPGWCoscStatInsertSQL:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
	debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
      }
    }
  }
  free(buffer);
  return retCode;
}
/*
	PDP stats to SQL in binay mode (exec prepared)
*/
int cdrPGWpdpStatInsertSQL( void *pdpStatDbIt, struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  struct _pdpStat *pdpStat=NULL;
  while( (pdpStat = myDB_getNextRow( pdpStatDbIt)))
  {
    retCode = pdpToSqlPrepared(pgCon,pdpStat,preparedName);
    if( retCode)
      break;
  }
  return retCode;
}
/*
	SUBSCRIBER tracking to SQL
*/
int substrackToStruct(struct _pgwRecord *pgwRecord, int substrkDB, int substrkTablesDB)
{
  int retCode=0;
  struct _ChangeOfCharCondition *cocc=NULL;
  unsigned int i=0,j=0;
  struct _userLocationInformation uli={{0}}, lastUli={{0}};
  struct _substrkSql *A=NULL;
  struct _substrkTableName *tableName=NULL;
  time_t t0, t1;
  time(&t0);
  t0 -= cdrDecParam.strkSqlLen;
  /*Subscriber tracking*/
  for(i=0,j=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
  {
    if(!cmpUli(cocc->userLocationInformation,uli))
    {
      cocc->userLocationInformation = pgwRecord->userLocationInformation;
      if(!cmpUli(cocc->userLocationInformation,uli))
	continue;
    }
    if(!cmpUli(cocc->userLocationInformation,lastUli))
    {
      continue;
    }
    lastUli = cocc->userLocationInformation;
    t1 = mktime(&cocc->changeTime);
    //char timeStr[64];
    //strftime(timeStr,sizeof(timeStr),"%Y-%m-%d %H:%M:%S",&cocc->changeTime);
    //debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, timeStr);
    if( t1 > t0)
    {
      if(!A)
      {
        A = calloc(1,sizeof(struct _substrkSql));
      }
      else
      {
        bzero(A,sizeof(struct _substrkSql));
      }
      A->tor = cocc->changeTime;
      A->isdn = bcdToUInt64Reverse(pgwRecord->servedMSISDN,MSISDN_LEN);
      strncpy(A->apnni,pgwRecord->accessPointNameNI,sizeof(A->apnni));
      A->rattype = pgwRecord->rATType;
      A->mcc = cocc->userLocationInformation.mcc;
      A->mnc = cocc->userLocationInformation.mnc;
      A->lac = cocc->userLocationInformation.lac;
      A->ci  = cocc->userLocationInformation.uliType.ecgi ? cocc->userLocationInformation.eci : cocc->userLocationInformation.ci;
      if(!myDB_insertRow(substrkDB,A))
      {
	if(!tableName)
	{
	  tableName = calloc(1,sizeof(struct _substrkTableName));
	}
	else
	{
	  bzero(tableName,sizeof(struct _substrkTableName));
	}
	strftime(tableName->tableName,SUBSTRK_TABLENAME_LEN,"cdr.substrk_%Y%m%d",&A->tor);
	if(!myDB_insertRow(substrkTablesDB,tableName))
	{
	  tableName->minTor = A->tor;
	  tableName->minTor.tm_hour = 0;
	  tableName->minTor.tm_min = 0;
	  tableName->minTor.tm_sec = 0;
	  tableName->maxTor = A->tor;
	  tableName->maxTor.tm_hour = 23;
	  tableName->maxTor.tm_min = 59;
	  tableName->maxTor.tm_sec = 59;
	  tableName = NULL;
	}
        A = NULL;
      }
      j++;
    }
  }
  if(A) free(A);
  if(tableName) free(tableName);
  /*Count inserts*/
  if(!retCode)
    retCode = j;
  return retCode;
}
int substrackToBuffer(struct _pgwRecord *pgwRecord, char *buffer, uint32_t *buffer_len, uint32_t *buffer_count, int tableNameDB)
{
  int retCode=0;
  struct _ChangeOfCharCondition *cocc=NULL;
  unsigned int i=0,j=0;
  struct _userLocationInformation uli={{0}}, lastUli={{0}};
  uint64_t msisdn/*, imsi, imei*/;
  char timeStr[32];
  char tableName[64];
  char *tableNameA=NULL;
  time_t t0, t1;
  time(&t0);
  t0 -= cdrDecParam.strkSqlLen;
  /*Subscriber tracking*/
  for(i=0,j=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
  {
    if(!cmpUli(cocc->userLocationInformation,uli))
    {
      cocc->userLocationInformation = pgwRecord->userLocationInformation;
      if(!cmpUli(cocc->userLocationInformation,uli))
	continue;
    }
    if(!cmpUli(cocc->userLocationInformation,lastUli))
    {
      continue;
    }
    lastUli = cocc->userLocationInformation;
    t1 = mktime(&cocc->changeTime);
    if( t1 > t0)
    {
      strftime(tableName,sizeof(tableName),"cdr.substrk_%Y%m%d%H",&cocc->changeTime);
      if(!tableNameA)
        tableNameA = malloc(SUBSTRK_TABLENAME_LEN);
      strcpy(tableNameA,tableName);
      if(!myDB_insertRow(tableNameDB,tableNameA))
      {
        tableNameA = NULL;
      }
      myStrftime_r( timeStr, sizeof(timeStr), &cocc->changeTime);
      msisdn = bcdToUInt64Reverse(pgwRecord->servedMSISDN,MSISDN_LEN);
      (*buffer_count) += snprintf(buffer+(*buffer_count),(*buffer_len)-(*buffer_count),\
        "'%s',%lu,%s,%u,%u,%u,%u,%u\n",\
        //"'%s',%s,%lu,%s,%u,%u,%u,%u,%u\n",
	timeStr,\
	//tableName,
	msisdn,\
	pgwRecord->accessPointNameNI,\
	pgwRecord->rATType,\
	cocc->userLocationInformation.mcc,\
	cocc->userLocationInformation.mnc,\
	cocc->userLocationInformation.lac,\
	cocc->userLocationInformation.uliType.ecgi ? cocc->userLocationInformation.eci : cocc->userLocationInformation.ci\
	);
      j++;
      //printf("llega hasta aqui %u\n",*buffer_count);
    }
  }
  if(tableNameA) free(tableNameA);
  /*Count inserts*/
  if(!retCode)
    retCode = j;
  return retCode;
}
int substrackToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName)
{
  int retCode=0;
  #define SUBSTRK_FIELDS 8
  struct _ChangeOfCharCondition *cocc=NULL;
  unsigned int i=0,j=0;
  struct _userLocationInformation uli={{0}}, lastUli={{0}};
  char *values[SUBSTRK_FIELDS];
  int len[SUBSTRK_FIELDS];
  int format[SUBSTRK_FIELDS];
  uint64_t msisdn/*, imsi, imei*/;
  uint32_t ci, lac;
  uint16_t mcc, mnc, rATType;
  //char sql[512];
  char logStr[512];
  char msgStr[512];
  char timeStr[32];
  time_t t0, t1;
  if( pgwRecord && pgCon && preparedName)
  {
    time(&t0);
    t0 -= cdrDecParam.strkSqlLen;
    /*Subscriber tracking*/
    for(i=0,j=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
    {
      if(!cmpUli(cocc->userLocationInformation,uli))
      {
        cocc->userLocationInformation = pgwRecord->userLocationInformation;
	if(!cmpUli(cocc->userLocationInformation,uli))
	  continue;
      }
      if(!cmpUli(cocc->userLocationInformation,lastUli))
      {
        continue;
      }
      lastUli = cocc->userLocationInformation;
      t1 = mktime(&cocc->changeTime);
      if( t1 > t0)
      {
        /*time of report*/
	myStrftime_r( timeStr, sizeof(timeStr), &cocc->changeTime);
	values[0] = myStrftime_r( timeStr, sizeof(timeStr), &cocc->changeTime);
	len[0] = 0;
	format[0] = 0;
	/*msisdn*/
	msisdn = bcdToUInt64Reverse(pgwRecord->servedMSISDN,MSISDN_LEN);
	msisdn = htobe64(bcdToUInt64Reverse(pgwRecord->servedMSISDN,MSISDN_LEN));
	values[1] = (char *)&msisdn;
	len[1] = sizeof(msisdn);
	format[1] = 1;
	/*apnni*/
	values[2] = pgwRecord->accessPointNameNI;
	len[2] = 0;
	format[2] = 0;
	/*rattype*/
	rATType = htobe16( pgwRecord->rATType);
	values[3] = (char *)&rATType;
	len[3] = sizeof(rATType);
	format[3] = 1;
	/*mcc*/
	mcc = htobe16( cocc->userLocationInformation.mcc);
	values[4] = (char *)&mcc;
	len[4] = sizeof(mcc);
	format[4] = 1;
	/*mcc*/
	mnc = htobe16( cocc->userLocationInformation.mnc);
	values[5] = (char *)&mnc;
	len[5] = sizeof(mnc);
	format[5] = 1;
	/*lac*/
	lac = htobe32( cocc->userLocationInformation.lac);
	values[6] = (char *)&lac;
	len[6] = sizeof(lac);
	format[6] = 1;
	/*ci*/
	ci = cocc->userLocationInformation.uliType.ecgi ? cocc->userLocationInformation.eci : cocc->userLocationInformation.ci;
	ci = htobe32( ci);
	values[7] = (char *)&ci;
	len[7] = sizeof(ci);
	format[7] = 1;
	retCode = myDB_PQexecPrepare( pgCon, preparedName, SUBSTRK_FIELDS, (const char * const*)values, len, format, msgStr, sizeof(msgStr));
	//snprintf(sql,sizeof(sql),"INSERT INTO %s_substrk(tor,msisdn,apnni,rattype,mcc,mnc,lac,ci) VALUES ('%s',%lu,'%s',%d,%d,%d,%u,%u)",preparedName,timeStr,msisdn,pgwRecord->accessPointNameNI,pgwRecord->rATType,cocc->userLocationInformation.mcc,cocc->userLocationInformation.mnc,cocc->userLocationInformation.lac,ci);
	//retCode = myDB_PQexec( pgCon, sql, msgStr, sizeof(msgStr));
	if( retCode < 0)
	{
	  retCode = -1;
	  snprintf(logStr,sizeof(logStr),"Error substrackToSqlPrepared:DB PQexecPrepare,Result=%d,%s",retCode,msgStr);
	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
	  break;
	}
	j++;
      }
    }
    /*Count inserts*/
    if(!retCode)
      retCode = j;
  }
  else
    retCode=-2;
  return retCode;
}
/*
	BEGIN
*/
int cdrPGWStatSQLBEGIN( struct _pgCon *pgCon)
{
  int retCode=0;
  char logStr[512];
  if( !pgCon)
    return -3;
  /*Insert Change of Char Condition statistics(list of traffic volumes)*/
  retCode = myDB_PQexec( pgCon, "BEGIN", logStr, sizeof(logStr));
  /*DB connection error*/
  if( retCode)
  {
    snprintf(logStr,sizeof(logStr),"Error:DB connection on BEGIN,Result=%d,%s",retCode,logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -4;
  }
  return retCode;
}
/*
	COMMIT
*/
int cdrPGWStatSQLCOMMIT( struct _pgCon *pgCon)
{
  int retCode=0;
  char logStr[512];
  if( !pgCon)
    return -3;
  /*Insert Change of Char Condition statistics(list of traffic volumes)*/
  retCode = myDB_PQexec( pgCon, "COMMIT", logStr, sizeof(logStr));
  /*DB connection error*/
  if( retCode)
  {
    snprintf(logStr,sizeof(logStr),"Error:DB connection on COMMIT,Result=%d,%s",retCode,logStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -4;
  }
  return retCode;
}
/*
	Prepare for PGWCDR combination
*/
int pgwcdrcmb_prepared( struct _pgCon *pgCon, char *tableName)
{
  int retCode=0;
  char sql[1024];
  char msgStr[1024];
  char logStr[1024];
  char preparedName[64];
  snprintf(preparedName,sizeof(preparedName),"%s_pgwcdrcmb",tableName);
  snprintf(sql,sizeof(sql),"INSERT INTO %s_pgwcdrcmb(id,lastupdate,pgwid,sgwid,apnid,dynamicAddressFlag,apnSelectionMode,cc,chChSelectionMode,iMSsignalingContext,cid) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",tableName);
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"Error:SQL prepare %s,MSG=%s",sql,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/*
	Prepare for LOTV stat
*/
int cdrPGWSQLPrepareCocc( struct _pgCon *pgCon, char *coccttn)
{
  int retCode=0;
  char sql[1024];
  char msgStr[1024];
  char logStr[1024];
  snprintf(sql, sizeof(sql),\
    "INSERT INTO %s_lotv(fecha,\
    pgwcdrcmbid,\
    withtraffic,\
    datavolumegprsuplink,\
    datavolumegprsdownlink,\
    qoSChange,\
    tariffTime,\
    recordClosure,\
    cGI_SAICHange,\
    rAIChange,\
    dT_Establishment,\
    dT_Removal,\
    eCGIChange,\
    tAIChange,\
    userLocationChange,\
    pdpStart,\
    pdpNormalStop,\
    pdpAbnormalStop,\
    pdpconcurrent) \
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",coccttn);
  if( myDB_prepareSQL( pgCon, coccttn, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"Error:SQL prepare %s,MSG=%s",sql,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/*
	Prepare for LOSD stat
*/
int cdrPGWSQLPrepareCosc( struct _pgCon *pgCon, char *coscttn)
{
  int retCode=0;
  char sql[11024];
  char msgStr[1024];
  char logStr[1024];
  char preparedName[64];
  snprintf(preparedName,sizeof(preparedName),"%s_losd",coscttn);
  snprintf(sql,sizeof(sql),"INSERT INTO %s_losd( fecha, datavolumefbcuplink, datavolumefbcdownlink, qoSChange, sGSNChange, sGSNPLMNIDChange, tariffTimeSwitch, pDPContextRelease, rATChange, serviceIdledOut, configurationChange, serviceStop, dCCATimeThresholdReached, dCCAVolumeThresholdReached, dCCAServiceSpecificUnitThresholdReached, dCCATimeExhausted, dCCAVolumeExhausted, dCCAValidityTimeout, dCCAReauthorisationRequest, dCCAContinueOngoingSession, dCCARetryAndTerminateOngoingSession, dCCATerminateOngoingSession, cGI_SAIChange, rAIChange, dCCAServiceSpecificUnitExhausted, recordClosure, timeLimit, volumeLimit, serviceSpecificUnitLimit, envelopeClosure, eCGIChange, tAIChange, userLocationChange, sessions, hits, pgwcdrcmbid, ratinggroup, serviceidentifier, resultcode, failurehandlingcontinue) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)", coscttn);
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"Error:%s,MSG=%s",__FUNCTION__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/*
	Prepare for susbcriber tracking
*/
int subsTrackSqlPrepare( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[1024];
  char msgStr[1024];
  char logStr[1024];
  //snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT id FROM %s.apntrk RIGHT OUTER JOIN (SELECT 0) AS q ON %s.apntrk.name=$3 LIMIT 1), ins1 AS (INSERT INTO %s.apntrk(name) SELECT $3 FROM sel1 WHERE sel1.id IS NULL RETURNING *), sel2 AS (SELECT sel1.id FROM sel1 WHERE sel1.id IS NOT NULL UNION DISTINCT SELECT ins1.id FROM ins1), sel3 AS (SELECT id FROM %s.celltrk RIGHT OUTER JOIN (SELECT 0) AS q ON %s.celltrk.rattype=$4 AND %s.celltrk.mcc=$5 AND %s.celltrk.mnc=$6 AND %s.celltrk.lac=$7 AND %s.celltrk.ci=$8 LIMIT 1), ins2 AS (INSERT INTO %s.celltrk(rattype,mcc,mnc,lac,ci) SELECT $4,$5,$6,$7,$8 FROM sel3 WHERE sel3.id IS NULL RETURNING *), sel4 AS (SELECT id FROM sel3 WHERE sel3.id IS NOT NULL UNION DISTINCT SELECT ins2.id FROM ins2) INSERT INTO %s.substrk(tor,isdn,apnid,cellid) SELECT $1,$2,sel2.id,sel4.id FROM sel2,sel4",cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema,cdrDecParam.statSqlSchema);
  snprintf(sql,sizeof(sql),"INSERT INTO %s_substrk(tor,msisdn,apnni,rattype,mcc,mnc,lac,ci) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);",preparedName);
  //snprintf(sql,sizeof(sql),"INSERT INTO substrack(time,msisdn,imsi,imei,rattype,apn,mcc,mnc,lac,ci) VALUES ($1::timestamp,$2::bigint,$3::bigint,$4::bigint,$5::smallint,$6::varchar,$7::smallint,$8::smallint,$9::int,$10::int);");
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"Error:SQL subsTrackSqlPrepare %s,MSG=%s",sql,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/*
	Prepare for Accounting start
*/
int acctStartPrepare( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[2024];
  char msgStr[2024];
  char logStr[2024];
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT COUNT(*) AS q FROM acct WHERE msisdn=$15::varchar AND apn=$16::varchar), ins1 AS (INSERT INTO acct(tststart,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid,msisdn,apn) SELECT $1::timestamp,$2::varchar,$3::varchar,$4::inet,$5::smallint,$6::smallint,$7::int,$8::int,$9::bigint,$10::smallint,0,0,$11::smallint,$12::inet,$13::inet,$14::varchar,$15::varchar,$16::varchar FROM sel1 WHERE sel1.q=0), upd1 AS (UPDATE acct SET (tststart,tstupdate,tststop,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid) = ($1::timestamp,NULL,NULL,$2::varchar,$3::varchar,$4::inet,$5::smallint,$6::smallint,$7::int,$8::int,$9::bigint,$10::smallint,0,0,$11::smallint,$12::inet,$13::inet,$14::varchar) FROM sel1 WHERE sel1.q<>0 AND msisdn=$15::varchar AND apn=$16::varchar AND ($1::timestamp>tststart OR tststart IS NULL) AND ($1::timestamp>tststop OR tststop IS NULL) AND ($1::timestamp>tstupdate OR tstupdate IS NULL) RETURNING *), sel2 AS (SELECT COUNT(*) AS q FROM upd1) UPDATE acct SET (tststart) = ($1::timestamp) FROM sel1,sel2 WHERE sel1.q<>0 AND sel2.q=0 AND msisdn=$15::varchar AND apn=$16::varchar AND chargingid=$9::bigint AND tststop IS NULL;");
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"%s,%s",__func__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
int acctStartToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName)
{
  int retCode=0;
  #define ACCTSTART_FIELDS 16
  //struct _ChangeOfCharCondition *cocc=NULL;
  //unsigned int i;
  //struct _userLocationInformation uli={{0}}/*, lastUli={{0}}*/;
  const char *values[ACCTSTART_FIELDS];
  char msisdn[20], imsi[20], imei[20];
  uint32_t ci, lac;
  uint64_t chargingid;
  uint16_t mcc, mnc, rATType, chargingCharacteristics;
  char logStr[512];
  char msgStr[512];
  char timeStr[32];
  char ip[64], gwaddress[64], servingNodeAddress[64];
  if( pgwRecord && pgCon && preparedName)
  {
    /*Prepared sizes*/
    const int len[ACCTSTART_FIELDS]={
	  0, // tststart::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  2, // mcc::smallint
	  2, // mnc::smallint
	  4, // lac::int
	  4, // ci::int
	  8, // chargingid::bigint
	  2, // chargingCharacteristics::smallint
	  2, // rattype::smallint
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0 // apn::varchar
    };
    /*Prepared format*/
    const int format[ACCTSTART_FIELDS]={
	  0, // tststart::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  1, // mcc::int
	  1, // mnc::int
	  1, // lac::int
	  1, // ci::int
	  1, // chargingid::bigint
	  1, // chargingCharacteristics::int
	  1, // rattype::int
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0  // apn::varchar
    };
    values[0] = myStrftime_r( timeStr, sizeof(timeStr), &pgwRecord->recordOpeningTime);
    bcdToStrReverse(imsi,sizeof(imsi),pgwRecord->servedIMSI,IMSI_LEN);
    values[1] = imsi;
    bcdToStrReverse(imei,sizeof(imei),pgwRecord->servedIMEI,IMEISV_LEN);
    values[2] = imei;
    values[3] = inet_ntop(AF_INET6,&pgwRecord->servedPDPPDNAddress,ip,sizeof(ip)); 
    mcc = htobe16( pgwRecord->userLocationInformation.mcc);
    values[4] = (char *)&mcc;
    mnc = htobe16( pgwRecord->userLocationInformation.mnc);
    values[5] = (char *)&mnc;
    lac = htobe32( pgwRecord->userLocationInformation.lac);
    values[6] = (char *)&lac;
    ci = pgwRecord->userLocationInformation.uliType.ecgi ? pgwRecord->userLocationInformation.eci : pgwRecord->userLocationInformation.ci;
    ci = htobe32( ci);
    values[7] = (char *)&ci;
    chargingid = htobe64(pgwRecord->chargingID);
    values[8] = (char *)&chargingid;
    chargingCharacteristics = htobe16( pgwRecord->chargingCharacteristics);
    values[9] = (char *)&chargingCharacteristics;
    rATType = htobe16( pgwRecord->rATType);
    values[10] = (char *)&rATType;
    values[11] = inet_ntop(AF_INET6,&pgwRecord->p_GWAddress,gwaddress,sizeof(gwaddress)); 
    values[12] = inet_ntop(AF_INET6,&pgwRecord->servingNodeAddress,servingNodeAddress,sizeof(servingNodeAddress)); 
    values[13] = pgwRecord->nodeID;
    bcdToStrReverse(msisdn,sizeof(msisdn),pgwRecord->servedMSISDN,MSISDN_LEN);
    values[14] = msisdn;
    values[15] = pgwRecord->accessPointNameNI;
    retCode = myDB_PQexecPrepare( pgCon, preparedName, ACCTSTART_FIELDS, values, len, format, msgStr, sizeof(msgStr));
    if( retCode)
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"Error %s,%s",__func__,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  else
    retCode=-2;
  return retCode;
}
/*
	Prepare for Accounting update
*/
int acctUpdatePrepare( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[2024];
  char msgStr[2024];
  char logStr[2024];
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT COUNT(*) AS q FROM acct WHERE msisdn=$15 AND apn=$16), ins1 AS (INSERT INTO acct(tstupdate,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid,msisdn,apn) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,0,$11,$12,$13,$14,$15,$16 FROM sel1 WHERE sel1.q=0), upd1 AS (UPDATE acct SET (tststart,tststop,tstupdate,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid) = (NULL,NULL,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,0,$11,$12,$13,$14) FROM sel1 WHERE sel1.q<>0 AND msisdn=$15 AND apn=$16 AND chargingid<>$9 AND ($1>tststart OR tststart IS NULL) AND ($1>tststop OR tststop IS NULL) AND ($1>tstupdate OR tstupdate IS NULL) RETURNING *), sel2 AS (SELECT COUNT(*) AS q FROM upd1) UPDATE acct SET (tstupdate,mcc,mnc,lac,ci,upoctets,downoctets,rattype,servingNodeAddress) = ($1,$5,$6,$7,$8,upoctets+$17,downoctets+$18,$11,$13) FROM sel1,sel2 WHERE sel1.q<>0 AND sel2.q=0 AND msisdn=$15 AND apn=$16 AND chargingid=$9 AND tststop IS NULL AND ($1>tstupdate OR tstupdate IS NULL);");
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"%s,%s",__func__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
int acctUpdateToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName)
{
  int retCode=0;
  #define ACCTUPDATE_FIELDS 18
  //struct _userLocationInformation uli={{0}}/*, lastUli={{0}}*/;
  const char *values[ACCTUPDATE_FIELDS];
  char msisdn[20], imsi[20], imei[20];
  uint32_t ci, lac;
  uint64_t chargingid=0, upOctets=0, downOctets=0;
  uint16_t mcc=0, mnc=0, rATType=0, chargingCharacteristics=0;
  char logStr[512];
  char msgStr[512];
  char timeStr[32];
  char ip[64], gwaddress[64], servingNodeAddress[64];
  if( pgwRecord && pgCon && preparedName)
  {
    /*Prepared sizes*/
    const int len[ACCTUPDATE_FIELDS]={
	  0, // tstupdate::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  2, // mcc::smallint
	  2, // mnc::smallint
	  4, // lac::int
	  4, // ci::int
	  8, // chargingid::bigint
	  2, // chargingCharacteristics::smallint
	  2, // rattype::smallint
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0, // apn::varchar
	  8, // upoctets::bigint
	  8  // downoctets::bigint
    };
    /*Prepared format*/
    const int format[ACCTUPDATE_FIELDS]={
	  0, // tstupdate::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  1, // mcc::int
	  1, // mnc::int
	  1, // lac::int
	  1, // ci::int
	  1, // chargingid::bigint
	  1, // chargingCharacteristics::int
	  1, // rattype::int
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0, // apn::varchar
	  1, // upoctets::bigint
	  1  // downoctets::bigint
    };
    values[0] = asctime_r( &pgwRecord->recordOpeningTime, timeStr);
    bcdToStrReverse(imsi,sizeof(imsi),pgwRecord->servedIMSI,IMSI_LEN);
    values[1] = imsi;
    bcdToStrReverse(imei,sizeof(imei),pgwRecord->servedIMEI,IMEISV_LEN);
    values[2] = imei;
    values[3] = inet_ntop(AF_INET6,&pgwRecord->servedPDPPDNAddress,ip,sizeof(ip)); 
    mcc = htobe16( pgwRecord->userLocationInformation.mcc);
    values[4] = (char *)&mcc;
    mnc = htobe16( pgwRecord->userLocationInformation.mnc);
    values[5] = (char *)&mnc;
    lac = htobe32( pgwRecord->userLocationInformation.lac);
    values[6] = (char *)&lac;
    ci = pgwRecord->userLocationInformation.uliType.ecgi ? pgwRecord->userLocationInformation.eci : pgwRecord->userLocationInformation.ci;
    ci = htobe32( ci);
    values[7] = (char *)&ci;
    chargingid = htobe64(pgwRecord->chargingID);
    values[8] = (char *)&chargingid;
    chargingCharacteristics = htobe16( pgwRecord->chargingCharacteristics);
    values[9] = (char *)&chargingCharacteristics;
    rATType = htobe16( pgwRecord->rATType);
    values[10] = (char *)&rATType;
    values[11] = inet_ntop(AF_INET6,&pgwRecord->p_GWAddress,gwaddress,sizeof(gwaddress)); 
    values[12] = inet_ntop(AF_INET6,&pgwRecord->servingNodeAddress,servingNodeAddress,sizeof(servingNodeAddress)); 
    values[13] = pgwRecord->nodeID;
    bcdToStrReverse(msisdn,sizeof(msisdn),pgwRecord->servedMSISDN,MSISDN_LEN);
    values[14] = msisdn;
    values[15] = pgwRecord->accessPointNameNI;
    values[16] = (char *)&upOctets;
    values[17] = (char *)&downOctets;
    retCode = myDB_PQexecPrepare( pgCon, preparedName, ACCTUPDATE_FIELDS, values, len, format, msgStr, sizeof(msgStr));
    if( retCode)
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"Error %s,%s",__func__,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  else
    retCode=-2;
  return retCode;
}
/*
	Prepare for Accounting stop
*/
int acctStopPrepare( struct _pgCon *pgCon, char *preparedName)
{
  int retCode=0;
  char sql[2024];
  char msgStr[2024];
  char logStr[2024];
  snprintf(sql,sizeof(sql),"WITH sel1 AS (SELECT COUNT(*) AS q FROM acct WHERE msisdn=$15 AND apn=$16), ins1 AS (INSERT INTO acct(tststop,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid,msisdn,apn) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,0,$11,$12,$13,$14,$15,$16 FROM sel1 WHERE sel1.q=0), upd1 AS (UPDATE acct SET (tststart,tstupdate,tststop,imsi,imei,ip,mcc,mnc,lac,ci,chargingid,chargingCharacteristics,upoctets,downoctets,rattype,gwaddress,servingNodeAddress,nodeid) = (NULL,NULL,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$17,$18,$11,$12,$13,$14) FROM sel1 WHERE sel1.q<>0 AND msisdn=$15 AND apn=$16 AND chargingid<>$9 AND ($1>tststart OR tststart IS NULL) AND ($1>tststop OR tststop IS NULL) AND ($1>tstupdate OR tstupdate IS NULL) RETURNING *), sel2 AS (SELECT COUNT(*) AS q FROM upd1) UPDATE acct SET (tststop,mcc,mnc,lac,ci,upoctets,downoctets,rattype,servingNodeAddress) = ($1,$5,$6,$7,$8,upoctets+$17,downoctets+$18,$11,$13) FROM sel1,sel2 WHERE sel1.q<>0 AND sel2.q=0 AND msisdn=$15 AND apn=$16 AND chargingid=$9;");
  if( myDB_prepareSQL( pgCon, preparedName, sql, msgStr, sizeof(msgStr)))
  {
    snprintf(logStr,sizeof(logStr),"%s,%s",__func__,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -1;
  }
  return retCode;
}
/*
	Accounting Stop to SQL prepared
*/
int acctStopToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName)
{
  int retCode=0;
  #define ACCTSTOP_FIELDS 18
  //struct _userLocationInformation uli={{0}}/*, lastUli={{0}}*/;
  const char *values[ACCTSTOP_FIELDS];
  char msisdn[20], imsi[20], imei[20];
  uint32_t ci, lac;
  uint64_t chargingid=0, upOctets=0, downOctets=0;
  uint16_t mcc=0, mnc=0, rATType=0, chargingCharacteristics=0;
  char logStr[512];
  char msgStr[512];
  char timeStr[32];
  char ip[64], gwaddress[64], servingNodeAddress[64];
  time_t stoptime;
  if( pgwRecord && pgCon && preparedName)
  {
    /*Prepared sizes*/
    const int len[ACCTSTOP_FIELDS]={
	  0, // tststop::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  2, // mcc::smallint
	  2, // mnc::smallint
	  4, // lac::int
	  4, // ci::int
	  8, // chargingid::bigint
	  2, // chargingCharacteristics::smallint
	  2, // rattype::smallint
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0, // apn::varchar
	  8, // upoctets::bigint
	  8  // downoctets::bigint
    };
    /*Prepared format*/
    const int format[ACCTSTOP_FIELDS]={
	  0, // tststop::timestamp
	  0, // imsi::varchar
	  0, // imei::varchar
	  0, // ip::inet
	  1, // mcc::int
	  1, // mnc::int
	  1, // lac::int
	  1, // ci::int
	  1, // chargingid::bigint
	  1, // chargingCharacteristics::int
	  1, // rattype::int
	  0, // gwaddress::inet
	  0, // servingNodeAddress::inet
	  0, // nodeid::varchar
	  0, // msisdn::varchar
	  0, // apn::varchar
	  1, // upoctets::bigint
	  1  // downoctets::bigint
    };
    stoptime = mktime(&pgwRecord->recordOpeningTime) + pgwRecord->duration;
    values[0] = ctime_r( &stoptime, timeStr);
    bcdToStrReverse(imsi,sizeof(imsi),pgwRecord->servedIMSI,IMSI_LEN);
    values[1] = imsi;
    bcdToStrReverse(imei,sizeof(imei),pgwRecord->servedIMEI,IMEISV_LEN);
    values[2] = imei;
    values[3] = inet_ntop(AF_INET6,&pgwRecord->servedPDPPDNAddress,ip,sizeof(ip)); 
    mcc = htobe16( pgwRecord->userLocationInformation.mcc);
    values[4] = (char *)&mcc;
    mnc = htobe16( pgwRecord->userLocationInformation.mnc);
    values[5] = (char *)&mnc;
    lac = htobe32( pgwRecord->userLocationInformation.lac);
    values[6] = (char *)&lac;
    ci = pgwRecord->userLocationInformation.uliType.ecgi ? pgwRecord->userLocationInformation.eci : pgwRecord->userLocationInformation.ci;
    ci = htobe32( ci);
    values[7] = (char *)&ci;
    chargingid = htobe64(pgwRecord->chargingID);
    values[8] = (char *)&chargingid;
    chargingCharacteristics = htobe16( pgwRecord->chargingCharacteristics);
    values[9] = (char *)&chargingCharacteristics;
    rATType = htobe16( pgwRecord->rATType);
    values[10] = (char *)&rATType;
    values[11] = inet_ntop(AF_INET6,&pgwRecord->p_GWAddress,gwaddress,sizeof(gwaddress)); 
    values[12] = inet_ntop(AF_INET6,&pgwRecord->servingNodeAddress,servingNodeAddress,sizeof(servingNodeAddress)); 
    values[13] = pgwRecord->nodeID;
    bcdToStrReverse(msisdn,sizeof(msisdn),pgwRecord->servedMSISDN,MSISDN_LEN);
    values[14] = msisdn;
    values[15] = pgwRecord->accessPointNameNI;
    values[16] = (char *)&upOctets;
    values[17] = (char *)&downOctets;
    retCode = myDB_PQexecPrepare( pgCon, preparedName, ACCTSTOP_FIELDS, values, len, format, msgStr, sizeof(msgStr));
    if( retCode)
    {
      retCode = -1;
      snprintf(logStr,sizeof(logStr),"Error %s,%s",__func__,msgStr);
      debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    }
  }
  else
    retCode=-2;
  return retCode;
}
/*

*/
//int cdrPGWRecordStatSQL( struct _pgwRecord *pgwRecord, struct _pgCon *pgCon, char *coccttn, char *coscttn)
//{
//  int retCode=0;
//  char sql[2048];
//  char logStr[1024];
//  char timestamp[64];
//  unsigned int i;
//  struct _ChangeOfCharCondition *cocc=NULL;
//  struct _ChangeOfServiceCondition *cosc=NULL;
//  struct _userLocationInformation localUli = {{0}};
//  time_t t;
//  struct tm timeTM;
//  if( pgwRecord)
//  {
//      /*listOfTrafficVolume statistics*/
//      for(i=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
//      {
//	t = mktime(&cocc->changeTime);
//	t = t-(t%statTimeDiv);
//	localtime_r( &t, &timeTM);
//	/*ULI*/
//	if( !memcmp( &cocc->userLocationInformation, &localUli, sizeof(struct _userLocationInformation)))
//	{
//	  cocc->userLocationInformation = pgwRecord->userLocationInformation;
//	  strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&cocc->changeTime);
//	  //snprintf(sql,sizeof(sql),"SELECT stat_cocc('%s','%s',%lu,%lu,'%s','%u%03u','%s',%d);",
//	  snprintf(sql,sizeof(sql),"INSERT INTO %s (fecha,apn,dataVolumeGPRSUplink,dataVolumeGPRSDownlink,nodeId,mcc,mnc,lac,ci,rATType) VALUES('%s','%s',%u,%u,'%s',%u,%u,%u,%u,%d);",
//	    coccttn,
//	    timestamp,
//	    pgwRecord->accessPointNameNI,
//	    cocc->dataVolumeGPRSUplink,
//	    cocc->dataVolumeGPRSDownlink,
//	    pgwRecord->nodeID,
//	    cocc->userLocationInformation.mcc,
//	    cocc->userLocationInformation.mnc,
//	    cocc->userLocationInformation.lac,
//	    cocc->userLocationInformation.ci ? cocc->userLocationInformation.ci : cocc->userLocationInformation.eci,
//	    pgwRecord->rATType
//	  );
//	  //myDB_PQexec( pgCon, sql, logStr, sizeof(logStr));
//	  retCode = myDB_PQexec( pgCon, sql, logStr, sizeof(logStr));
//	  if( retCode)
//	  {
//	    retCode = -2;
//	    snprintf(logStr,sizeof(logStr),"Error:DB exec,Result=%d,%s",retCode,logStr);
//	    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	    break;
//	  }
//	}
//      }
//      /*listOfServiceData statistics*/
//      for(i=0,cosc=&pgwRecord->listOfServiceData; cosc&&(i<pgwRecord->listOfServiceDataCount); cosc=cosc->next, i++)
//      {
//	t = mktime(&cosc->timeOfReport);
//	t = t-(t%statTimeDiv);
//	localtime_r( &t, &timeTM);
//	/*ULI*/
//	if( !memcmp( &cosc->userLocationInformation, &localUli, sizeof(struct _userLocationInformation)))
//	{
//	  cosc->userLocationInformation = pgwRecord->userLocationInformation;
//	}
//	strftime(timestamp,sizeof(timestamp),"%Y-%m-%d %H:%M:%S",&cosc->timeOfReport);
//	//snprintf(sql,sizeof(sql),"SELECT stat_cocc('%s','%s',%lu,%lu,'%s','%u%03u','%s',%d);",
//	snprintf(sql,sizeof(sql),"INSERT INTO %s (fecha,apn,datavolumeFBCUplink,datavolumeFBCDownlink,nodeId,mcc,mnc,lac,ci,rATType,ratingGroup,serviceIdentifier,sessions,resultCode,failurehandlingcontinue,timeusage) VALUES('%s','%s',%u,%u,'%s',%u,%u,%u,%u,%d,%u,%u,1,%u,'%s',%u);",
//	  coscttn,
//	  timestamp,
//	  pgwRecord->accessPointNameNI,
//	  cosc->datavolumeFBCUplink,
//	  cosc->datavolumeFBCDownlink,
//	  pgwRecord->nodeID,
//	  cosc->userLocationInformation.mcc,
//	  cosc->userLocationInformation.mnc,
//	  cosc->userLocationInformation.lac,
//	  cosc->userLocationInformation.ci ? cosc->userLocationInformation.ci : cosc->userLocationInformation.eci,
//	  pgwRecord->rATType,
//	  cosc->ratingGroup,
//	  cosc->serviceIdentifier,
//	  /*cosc->sessions,*/
//	  cosc->resultCode,
//	  cosc->failureHandlingContinue ? "t":"f",
//	  cosc->timeUsage
//	);
//	retCode = myDB_PQexec( pgCon, sql, logStr, sizeof(logStr));
//	if( retCode)
//	{
//	  retCode = -2;
//	  snprintf(logStr,sizeof(logStr),"Error:DB myDB_PQexec,Result=%d,%s",retCode,logStr);
//	  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
//	  break;
//	}
//      }
//  }
//  return retCode;
//}
int cdrPGWStatSQLResume( struct _pgCon *pgCon, char *coccttn, char *coscttn)
{
  int retCode=0;
  char logStr[512];
  char msgStr[512];
  char sql[512];
  if( !pgCon)
    return -3;
  /*Get affected rows for COCC*/
  //snprintf(sql,sizeof(sql),"SELECT * FROM %s",coccttn);
  snprintf(sql,sizeof(sql),"SELECT fecha,apn,nodeid,rattype,mcc,mnc,lac,ci,sum(dataVolumeGPRSUplink),sum(dataVolumeGPRSDownlink) FROM %s GROUP BY fecha,apn,nodeid,rattype,mcc,mnc,lac,ci",coccttn);
  myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  snprintf(logStr,sizeof(logStr),"Insert to %s Result=%s",coccttn,msgStr);
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  /*Get affected rows for COSC*/
  //snprintf(sql,sizeof(sql),"SELECT * FROM %s",coscttn);
  snprintf(sql,sizeof(sql),"SELECT fecha,apn,nodeid,rattype,mcc,mnc,lac,ci,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue,sum(datavolumefbcuplink),sum(datavolumefbcdownlink),sum(sessions),sum(timeusage) FROM %s GROUP BY fecha,apn,nodeid,rattype,mcc,mnc,lac,ci,ratinggroup,serviceidentifier,resultcode,failurehandlingcontinue",coscttn);
  myDB_getSQL( pgCon, sql, msgStr, sizeof(msgStr));
  snprintf(logStr,sizeof(logStr),"Insert to %s Result=%s",coscttn,msgStr);
  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
  retCode = myDB_PQexec( pgCon, "COMMIT", msgStr, sizeof(msgStr));
  /*DB connection error*/
  if( retCode)
  {
    snprintf(logStr,sizeof(logStr),"Error:DB connection on COMMIT,Result=%d,%s",retCode,msgStr);
    debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, logStr);
    retCode = -4;
  }
  return retCode;
}
