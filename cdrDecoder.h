#ifndef _CDRDECODER_H_
#define _CDRDECODER_H_ 1
//#include "mySubscriber.h"
//#include "3gppCdr.h"
#include "time.h"
#include "tc-oam.h"
#include "stdint.h"
#include "tc-mydb.h"
#define _CDR_DECODER_LOG_NAME_ "cdrDecoder"
/*Counters*/
extern long globalCpuNum;
/*Structs*/
struct _substrkSql {
  unsigned start:1;
  unsigned stop:1;
  uint64_t imei;
  uint64_t uplink;
  uint64_t downlink;
  struct tm tor;
  uint64_t isdn;
  char apnni[64];
  uint8_t rattype;
  uint16_t mcc;
  uint16_t mnc;
  uint32_t lac;
  uint32_t ci;
};
struct _gwaddress {
  uint16_t id;
  struct _plmnId plmn;
  char nodeId[64];
  struct in6_addr gwaddress;
};
struct _snaddress {
  uint16_t id;
  struct _plmnId plmn;
  struct in6_addr snaddress;
  uint16_t sntype;
};
struct _nodeId {
  uint16_t id;
  struct _plmnId plmn;
  char nodeName[64];
};
struct _cdrCi {
  uint32_t id;
  uint8_t ratType;
  struct _userLocationInformation uli;
};
struct _apnId {
  uint16_t id;
  struct _plmnId plmn;
  char apnNi[63];
};
#define lotvTableLen 32 
#define losdTableLen 32
struct _losdTables{
  char name[losdTableLen];
  struct tm tm;
};
struct _lotvTables{
  char name[lotvTableLen];
  struct tm tm;
};
/* cdr_pgwcdrcmb */
struct _cdr_pgwcdrcmb{
  struct tm tst_tm;
  uint64_t id;
  uint32_t cdrCi_id;
  uint16_t cc;
  uint16_t gwaddress_id;
  uint16_t snaddress_id;
  uint16_t apnId_id;
  unsigned dynamicAddressFlag:1;
  unsigned iMSsignalingContext:1;
  uint16_t apnSelectionMode;
  uint16_t chChSelectionMode;
};
#define SUBSTRK_BUFFER 1024*1024
#define SUBSTRK_TABLENAME_LEN 64
struct _substrkTableName{
  char tableName[SUBSTRK_TABLENAME_LEN];
  struct tm minTor;
  struct tm maxTor;
};
extern int copyCdrSubsTrackTables(struct _pgCon *pgCon, void *it_substrkDB, void *substrkTableNameIt);
extern int substrackToStruct(struct _pgwRecord *pgwRecord, int substrkDB, int substrkTablesDB);
extern void pqnotifyreceiver(void *arg, const PGresult *res);
extern int copyCdrSubsTrack(struct _pgCon *pgCon, char *tableName, char *buffer, uint32_t buffer_count, void *substrkTableNameIt);
extern int substrackToBuffer(struct _pgwRecord *pgwRecord, char *buffer, uint32_t *buffer_len, uint32_t *buffer_counti, int tableNameDB);
extern int insertCdrSubsTrack(struct _pgCon *pgCon, char *tableName);
//extern int subsTrackSqlTmpTable(struct _pgCon *pgCon,char *tableName, struct tm minTor, struct tm maxTor);
extern int subsTrackSqlTmpTable(struct _pgCon *pgCon, void *substrkTableNameIt);
extern int gwAddress_tmpTable(struct _pgCon *pgCon, char *tableName);
extern int snAddress_tmpTable(struct _pgCon *pgCon, char *tableName);
extern int apn_tmpTable( struct _pgCon *pgCon, char *tableName);
extern int addCdrApn_substrk(struct _pgCon *pgCon, void *it_apnDB, char *tableName);
extern int addPgwCdrCmb(struct _pgCon *pgCon, void *it, char *tableName);
extern int addCdrCi_substrk(struct _pgCon *pgCon, void *it_ciDB, char *tableName);
extern int addCdrCi(struct _pgCon *pgCon, void *it_ciDB, char *tableName);
extern int addCdrGwAddress(struct _pgCon *pgCon, void *it_gwaddressDB, char *tableName);
extern int addCdrSnAddress(struct _pgCon *pgCon, void *it_snaddressDB, char *tableName);
extern int addCdrNodeId(struct _pgCon *pgCon, void *it_nodeDB);
extern int addCdrApn(struct _pgCon *pgCon,void *it_apnDB, char *tableName);
extern int addCdrPlmn(struct _pgCon *pgCon, void *it_plmnDB);
extern int addCdrMcc(struct _pgCon *pgCon,void *it_mccDB);
/*PDP STAT*/
struct _pdpStat{
  time_t tst;
  char apn[65];
  struct in6_addr gwAddr;
  struct in6_addr servingNodeAddr;
  uint8_t rATType;
  uint8_t servingNodeType;
  uint16_t mcc;
  uint16_t mnc;
  float pdpConcurrent;
  uint32_t pdpStart;
  uint32_t pdpUpdate;
  uint32_t pdpStop;
};
extern int addPdpStat( struct _pdpStat *, int dbId);
extern int cmpPdpStat( void *a, void *b);
extern int cdrPdpStatClean( struct _pdpStat *);
/*NETWORK PAYLOAD USAGE*/
struct _pgwRecordCOCC{
  //unsigned char busy;
  char apnni[64];
  struct in6_addr gwaddr;
  uint16_t gwmcc;
  uint16_t gwmnc;
  struct in6_addr snaddr;
  uint8_t sntype;
  uint16_t snmcc;
  uint16_t snmnc;
  unsigned withtraffic:1;
  uint8_t rATType;
  struct _userLocationInformation uLI;
  uint16_t cc;
  uint8_t apnselectionmode;
  uint8_t chchselectionmode;
  unsigned dynamicaddressflag:1;
  unsigned imssignalingcontext:1;
  uint32_t pgwcdrcmbid;
  //char apn[63];
  //char nodeId[20];
  //uint32_t rATType;
  uint64_t dataVolumeGPRSUplink;
  uint64_t dataVolumeGPRSDownlink;
  uint64_t qoSChange;
  uint64_t tariffTime;
  uint64_t recordClosure;
  uint64_t cGI_SAICHange;
  uint64_t rAIChange;
  uint64_t dT_Establishment;
  uint64_t dT_Removal;
  uint64_t eCGIChange;
  uint64_t tAIChange;
  uint64_t userLocationChange;
  struct tm changeTime;
  //uint32_t chargingCharacteristic;
  ///struct in6_addr gwAddr;
  //struct in6_addr snAddr;
  //uint16_t snType;
  uint64_t pdpStart;
  uint64_t pdpNormalStop;
  uint64_t pdpAbnormalStop;
  float pdpconcurrent;
  //uint64_t servingNodeChange;
  //uint64_t managementIntervention;
  //uint64_t intraSGSNIntersystemChange;
  //uint64_t rATChange;
  //uint64_t mSTimeZoneChange;
  //uint64_t sGSNPLMNIDChange;
  //uint64_t unauthorizedRequestingNetwork;
  //struct _pgwRecordCOCC *next;
};
extern int cdrPGWRecordCOCCClean( struct _pgwRecordCOCC *);
/*SERVICE AWARE USAGE*/
struct _pgwRecordCOSC{
  uint32_t pgwcdrcmbid;
  //unsigned char busy;
  char nodeId[20];
  struct _userLocationInformation uLI;
  //int rATType;
  struct tm timeOfReport;
  char apnni[64];
  uint16_t rATType;
  struct in6_addr gwaddr;
  uint16_t gwmcc;
  uint16_t gwmnc;
  struct in6_addr snaddr;
  uint8_t sntype;
  uint16_t snmcc;
  uint16_t snmnc;
  uint16_t cc;
  char crbn[64];
  uint8_t failureHandlingContinue;
  uint32_t serviceIdentifier;
  uint32_t ratingGroup;
  uint32_t resultCode;
  uint64_t datavolumeFBCUplink;
  uint64_t datavolumeFBCDownlink;
  uint64_t timeusage;
  uint64_t qoSChange;
  uint64_t sGSNChange;
  uint64_t sGSNPLMNIDChange;
  uint64_t tariffTimeSwitch;
  uint64_t pDPContextRelease;
  uint64_t rATChange;
  uint64_t serviceIdledOut;
  uint64_t configurationChange;
  uint64_t serviceStop;
  uint64_t dCCATimeThresholdReached;
  uint64_t dCCAVolumeThresholdReached;
  uint64_t dCCAServiceSpecificUnitThresholdReached;
  uint64_t dCCATimeExhausted;
  uint64_t dCCAVolumeExhausted;
  uint64_t dCCAValidityTimeout;
  uint64_t dCCAReauthorisationRequest;
  uint64_t dCCAContinueOngoingSession;
  uint64_t dCCARetryAndTerminateOngoingSession;
  uint64_t dCCATerminateOngoingSession;
  uint64_t cGI_SAIChange;
  uint64_t rAIChange;
  uint64_t dCCAServiceSpecificUnitExhausted;
  uint64_t recordClosure;
  uint64_t timeLimit;
  uint64_t volumeLimit;
  uint64_t serviceSpecificUnitLimit;
  uint64_t envelopeClosure;
  uint64_t eCGIChange;
  uint64_t tAIChange;
  uint64_t userLocationChange;
  uint64_t hits;
  float sessions;
  //union _ServiceConditionChange serviceConditionChange;
  //struct _pgwRecordCOSC *next;
};
extern struct _pgwRecordCOSC firstPgwRecordCOSC;
struct _pgwCdrStatRes {
  int retCode;
  unsigned int totalCocc;
  unsigned int totalCoccInsert;
  unsigned int totalCosc;
  unsigned int totalCoscInsert;
};
struct _cdrCompactArg {
  unsigned run:1;
  unsigned pause:1;
  unsigned maintainPsStat:1;
  unsigned maintainLotv:1;
  unsigned maintainLotvTmp:1;
  unsigned maintainLotvMean:1;
  unsigned maintainLosd:1;
  unsigned maintainLosdTmp:1;
  unsigned maintainLosdMean:1;
  unsigned maintainPdpstat:1;
  unsigned maintainSubsTrk:1;
  unsigned int id;
  struct _pgCon *pgCon;
  struct _pgCon *pgConTrk;
  pthread_t ptid;
  unsigned int jobCount;
  unsigned int sleep;
  unsigned int lotvRows;
  unsigned int losdRows;
};
struct _msisdn_cdr {
  uint32_t id;
  uint64_t msisdn;
};
struct _imsi_cdr {
  uint32_t id;
  uint64_t imsi;
};
struct _imei_cdr {
  uint32_t id;
  uint64_t imei;
};
struct _gw_cdr {
  uint8_t id;
  unsigned mcc:12;
  unsigned mnc:12;
  uint64_t imei;
};
struct _apn_cdr {
  uint8_t id;
  unsigned mcc:12;
  unsigned mnc:12;
  uint8_t len;
  char apn;
};
struct _cell_cdr {
  uint32_t id;
  unsigned rattype:4;
  unsigned ci:28;
  unsigned mnc:12;
  unsigned mcc:12;
  struct {
    unsigned cgi:1;
    unsigned sai:1;
    unsigned rai:1;
    unsigned tai:1;
    unsigned ecgi:1;
    unsigned lai:1;
    unsigned :2;
  }uliType;
  uint16_t lac;
};
struct _substrack_cdr {
  time_t chT;
  uint32_t msisdn_id;
  uint32_t imsi_id;
  uint32_t imei_id;
  uint32_t cell_id;
  uint32_t dataVolumeGPRSUplink;
  uint32_t dataVolumeGPRSDownlink;
  uint8_t apn_id;
  uint8_t changeCondition;
  uint8_t causeForRecClosing;
};
struct _lotv_cdr {
  time_t chT;
  uint8_t gw_id;
  uint8_t apn_id;
  uint8_t changeCondition;
  uint8_t causeForRecClosing;
  uint32_t cell_id;
  uint64_t dataVolumeGPRSUplink;
  uint64_t dataVolumeGPRSDownlink;
  uint32_t timeUsage;
  uint32_t createPDPContext;
  uint16_t coccRecords;
};
//extern int cdrPGWRecordCOSCClean( struct _pgwRecordCOSC *);
extern unsigned int pgwRecordCOSCCount;
/*subscriber trace*/
extern uint64_t *subsTrace;
extern unsigned char subsTraceCount;
/*Subscriber DB*/
extern int subscriberDB;
enum {
  cdrPGWRecordStatInfoType_cocc=1,
  cdrPGWRecordStatInfoType_cosc,
};
/*PostgreSQL connection*/
extern struct _pgCon *globalPgCon;
extern struct _pgCon *cdrCompactPgCon;
/*CDR decoder OAM param*/
#define MAXCDRDIRCOUNT 256
#define SUBSTRKSQLTABLEPREFIX "substrk"
struct _cdrDecParam{
  unsigned decoderOn:1;
  unsigned maintainPsStat:1;
  unsigned maintainSubstrk:1;
  unsigned pwgStat:1;
  unsigned subsTrace:1;
  unsigned subsTrack:1;
  unsigned subsTrackSql:1;
  unsigned subsTrackFile:1;
  unsigned allSubsTrace:1;
  unsigned subsDb:1;
  unsigned delFile:1;
  unsigned one:1;
  unsigned subsNotify:1;
  unsigned short statSqlGroup;
  unsigned short strkSqlGroup;
  time_t strkSqlLen;
  time_t strkFileLen;
  char *statSqlSchema;
  char *cdrDir[MAXCDRDIRCOUNT];
  unsigned char errorFileDir;
  unsigned char revertedFileDir;
};
struct _cdrPGWRecordStatParams{
  struct _pgwRecord *pgwRecord;
  struct _cdr_pgwcdrcmb *cdr_pgwcdrcmb;
  int coccDB;
  int coscDB;
  int pdpStatDB;
  int ciDB;
  int lotvTablesDB;
  int losdTablesDB;
  int pgwCdrEventTablesDB;
  int pgwCdrCmbDB;
  uint64_t *pgwRecordTablesId;
  uint32_t *cdrCi_id;
};
#define pgwcdrEventTableLen 16
extern int cdrCiCreateTmpTable( struct _pgCon *pgCon, char *tableName);
extern int addLosdTables(struct _pgCon *pgCon, void *it_losdTablesDB);
extern int addLotvTables(struct _pgCon *pgCon, void *it_lotvTablesDB);
extern struct _cdrDecParam cdrDecParam;
extern int cdrDecoderOamParser( struct _oamCommand *oamCmd, char *returnMsg, unsigned int msgLen);
extern int pgwCDRSubsDB( int dbId, struct _pgwRecord *pgwRecord);
extern int pgwCDRSubsTrc( struct _pgwRecord *pgwRecord);
extern int pgwCDRSubsTrk( struct _pgwRecord *pgwRecord);
extern int pgwCDRSubsTrkSql( struct _pgCon *, struct _pgwRecord *pgwRecord);
/*PGW records statistics functions*/
extern int cmpPgwStatCOCC( void *p1, void *p2);
extern int cmpPgwStatCOSC( void *p1, void *p2);
extern int cdrPGWCoccCreateTmpTable( struct _pgCon *pgCon, char *tableName);
extern int cdrPGWCoscCreateTmpTable( struct _pgCon *pgCon, char *tableName);
extern int pgwcdrcmb_CreateTmpTable( struct _pgCon *pgCon, char *pgwcdrcmb_TmpTable);
extern int cdrPGWCoccStatInsertSQL( void *coccIt, struct _pgCon *pgCon, char *tableName, void *tableIt);
extern int cdrPGWCoscStatInsertSQL( void *coscIt, struct _pgCon *pgCon, char *tableName, void *tableIt);
extern int cdrPGWpdpStatInsertSQL( void *pdpStatDbIt, struct _pgCon *pgCon, char *preparedName);
extern int cdrPGWRecordStat( struct _cdrPGWRecordStatParams *);
extern int cdrPGWRecordStatInsertSQL( int coccDB, int coscDB);
extern int cdrPGWRecordGetSQL( struct _pgwRecord *pgwRecord, char *cmd, unsigned int len);
extern unsigned int cdrPgwRecordStatClean( int coccDB, int coscDB, int  pdpStatDB);
extern int cdrPGWStatSQLCOMMIT( struct _pgCon *pgCon);
extern int cdrPGWStatSQLBEGIN( struct _pgCon *pgCon);
extern int cdrPGWRecordStatSQL( struct _pgwRecord *pgwRecord, struct _pgCon *pgCon, char *coccttn, char *coscttn);
extern int cdrPGWStatSQLResume( struct _pgCon *pgCon, char *coccttn, char *coscttn);
extern int pgwcdrcmb_prepared( struct _pgCon *pgCon, char *tableName);
extern int cdrPGWSQLPrepareCocc( struct _pgCon *pgCon, char *coccttn);
extern int cdrPGWSQLPrepareCosc( struct _pgCon *pgCon, char *coscttn);
extern void *cdrCompact( void *cdrCompactArg);
extern int subsTrackSqlPrepare( struct _pgCon *pgCon, char *preparedName);
extern int substrackToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName);
extern int acctStartPrepare( struct _pgCon *pgCon, char *preparedName);
extern int acctUpdatePrepare( struct _pgCon *pgCon, char *preparedName);
extern int acctStopPrepare( struct _pgCon *pgCon, char *preparedName);
extern int acctStartToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName);
extern int acctStopToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName);
extern int acctUpdatedToSqlPrepared(struct _pgCon *pgCon, struct _pgwRecord *pgwRecord, char *preparedName);
extern int cmpUli(struct _userLocationInformation uliA, struct _userLocationInformation uliB);
extern int createPdpTable(struct _pgCon *pgCon, char *tableName);
extern int deletePdpTable(struct _pgCon *pgCon, char *tableName);
extern int subscribePdpTable(struct _pgCon *pgCon, char *tableName);
extern int commitPdpTable(struct _pgCon *pgCon, char *tableName);
extern int declinePdpTable(struct _pgCon *pgCon, char *tableName);
extern int pdpPrepare( struct _pgCon *pgCon, char *preparedName);
extern int pdpPrepareDealloc( struct _pgCon *pgCon, char *preparedName);
extern int pdpToSqlPrepared(struct _pgCon *pgCon, struct _pdpStat *pdpStat, char *preparedName);



/*In file built-in DB*/
/*MSISDN*/
extern pthread_mutex_t msisdn_mut;
extern uint32_t msisdnId;
//extern unsigned char lockMsisdnTable;
//extern int msisdnCdrTableId;
extern int cmp_subsIdTable(void *pA, void *pB);
extern uint32_t getSubsId( char *tableName, uint64_t msisdn, uint32_t *serial, pthread_mutex_t *mutex);
extern uint32_t querySubsId( char *tableName, uint64_t msisdn, pthread_mutex_t *mutex);
extern uint64_t querySubsMsisdn( char *tableName, uint32_t id, pthread_mutex_t *mutex);
extern int cmp_subsIdTable(void *pA, void *pB);
extern int getMaxSubsId( char *tableName, uint32_t *id);
/*APN*/
extern pthread_mutex_t apnMutex;
extern uint16_t apnId;
//extern unsigned char lockApnTable;
//extern int apnCdrTableId;
extern int cmp_apnIdTable(void *pA, void *pB);
extern int getMaxApnId( char *tableName, uint16_t *id);
extern uint16_t getApnId( char *tableName, char *apn, uint16_t *serial, pthread_mutex_t *mutex;);
extern int queryApn( char *tableName, char *apn, unsigned int len, uint32_t apnid);
extern int queryApnTable( char *tableName, struct _oamCommand *oam);
/*CELL*/
extern pthread_mutex_t cellMutex;
struct _cellTable {
  uint32_t id;
  uint16_t mcc;
  uint16_t mnc;
  uint32_t ci;
  uint16_t lac;
  uint8_t ratt;
};
//extern int cellCdrTableId;
extern uint32_t cellId;
//extern unsigned char lockCellTable;
extern int cmp_cellIdTable( void *pA, void *pB);
extern int getMaxCellId( char *tableName, uint32_t *id);
extern uint32_t getCellId( char *tableName, struct _cellTable *cellStruct, uint32_t *serial, pthread_mutex_t *mutex);
extern int queryCell( char *tableName, struct _cellTable *cellTable);
extern int queryCellTable( char *tableName, struct _oamCommand *oam);
/*USER COMBINATION*/
struct _userComb {
  time_t time;
  struct {
    unsigned on:1;
    unsigned ipv4:1;
    unsigned ipv6:1;
    unsigned :5;
  } flags;
  uint32_t msisdnId;
  uint64_t imei;
  uint64_t imsi;
  uint64_t msisdn;
  uint16_t apnId;
  struct in_addr ip;
  struct in6_addr ipv6;
  struct _userComb *next;
};
/*Total Octets*/
struct _totalOctets {
  uint64_t upOctets;
  uint64_t downOctets;
};
//extern int userCombTableId;
extern int cmp_userCombTable( void *pA, void *pB);
extern int addUserComb( struct _userComb *userComb);
extern struct _userComb *getUserComb( char *tableName, struct _userComb *userComb);
extern int freeUserComb( struct _userComb *userComb);
extern int queryCmb( struct _oamCommand *oamCmd, uint64_t msisdn, uint64_t imsi, uint64_t imei, struct tm *fromTm, struct tm *toTm);
/*SUBSTRACK*/
struct _substrack {
  time_t tOR;
  //uint32_t msisdnId;
  uint64_t msisdn;
  uint16_t apnId;
  uint32_t cellId;
  uint64_t upOctets;
  uint64_t downOctets;
  struct _substrack *next;
};
extern int addSubstrack( struct _substrack *substrack);
extern int freeSubstrack( struct _substrack *substrack);
//extern time_t substrackLen;
extern int getSubsTrack(struct _oamCommand *oamCmd, uint64_t msisdn, struct tm *fromTM, struct tm *toTM);
/*Notification*/
struct _notifyInfo {
  struct {
    unsigned msisdn:1;
    unsigned imei:1;
  } flags;
  uint64_t msisdn;
  uint64_t imei;
  uint8_t emailCount;
  char **email;
};
extern int addNotifySubscriber( struct _notifyInfo *notifyInfo);
extern int delNotifySubscriber( struct _notifyInfo *notifyInfo);
extern int cmp_notifyCombTable( void *pA, void *pB);
extern void freeNotifyInfo( struct _notifyInfo *notifyInfo);
extern int getNotifyInfoMsisdn( struct _notifyInfo *notifyInfo);
extern int listNotifyInfo( struct _oamCommand *oamCmd);
/*INIT*/
extern int cdrBuiltInDb_init(char *msg, int msgLen);
extern int cdrDb_id;
extern char cdrDbDir[MYDBFILE_MAXTABLEDIRLEN];
#define CDRBUILTINDB_DEFAULT_DBNAME "cdr"
#define CDRBUILTINDB_DEFAULT_MSISDNTABLE "msisdn_cdr"
#define CDRBUILTINDB_DEFAULT_APNTABLE "apn_cdr"
#define CDRBUILTINDB_DEFAULT_CELLTABLE "cell_cdr"
#define CDRBUILTINDB_DEFAULT_USERCOMBTABLE "usercomb_cdr"
#define CDRBUILTINDB_DEFAULT_NOTIFYTABLE "snotify"

#endif
