#include "cdrDecoder.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include "tc-mydb.h"
#include "time.h"
#include "tc-mydebug.h"
#include <arpa/inet.h>

int pgwCDRSubsTrk( struct _pgwRecord *pgwRecord)
{
  int retCode=0;
  char logStr[1024];
  //int32_t causeForRecClosing=0;
  struct _ChangeOfCharCondition *cocc=NULL;
  unsigned int i;
  struct _userLocationInformation uli={{0}}, lastUli={{0}};
  struct _substrack substrack={0};
  struct _userComb *userComb=pgwRecord->extra1;
  struct _cellTable cellTable={0};
  if( pgwRecord)
  {
    //causeForRecClosing = pgwRecord->causeForRecClosing;
    /*Subscriber tracking*/
    //substrack.msisdnId = userComb->msisdnId;
    substrack.msisdn = userComb->msisdn;
    substrack.apnId = userComb->apnId;
    cellTable.ratt = pgwRecord->rATType;
    /**/
    for(i=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
    {
      if(!cmpUli(cocc->userLocationInformation,uli))
      {
        cocc->userLocationInformation = pgwRecord->userLocationInformation;
	if(!cmpUli(cocc->userLocationInformation,uli))
	  continue;
      }
      if( !cmpUli(cocc->userLocationInformation,lastUli))
      {
	substrack.upOctets += cocc->dataVolumeGPRSUplink;
	substrack.downOctets += cocc->dataVolumeGPRSDownlink;
	continue;
      }
      lastUli = cocc->userLocationInformation;
      /*Logging*/
      //snprintf(logStr,sizeof(logStr),
      //  /*time,msisdn,imsi,imei,rattype,apn,mcc,mnc,lac,ci*/
      //  "%s,%lu,%lu,%lu,%d,%s,%s\n",
      //  myStrftime(&cocc->changeTime),
      //  bcdToUInt64Reverse(pgwRecord->servedMSISDN,MSISDN_LEN),
      //  bcdToUInt64Reverse(pgwRecord->servedIMSI,IMSI_LEN),
      //  bcdToUInt64Reverse(pgwRecord->servedIMEI,IMEISV_LEN),
      //  pgwRecord->rATType,
      //  pgwRecord->accessPointNameNI,
      //  uliPrintCsv(auxStr,sizeof(auxStr),&cocc->userLocationInformation)
      //  );
      //debugLoggingAI(DEBUG_ERROR,"cdrDecPgwRecSubsTrack",logStr);
      /*Built-In DB*/
      substrack.tOR = mktime(&cocc->changeTime);
      cellTable.mcc = cocc->userLocationInformation.mcc;
      cellTable.mnc = cocc->userLocationInformation.mnc;
      cellTable.lac = cocc->userLocationInformation.lac;
      cellTable.ci = cocc->userLocationInformation.eci ? cocc->userLocationInformation.eci : cocc->userLocationInformation.ci;
      substrack.cellId = getCellId( CDRBUILTINDB_DEFAULT_CELLTABLE,&cellTable,&cellId,&cellMutex);
      if( !substrack.cellId) return -5;
      substrack.upOctets += cocc->dataVolumeGPRSUplink;
      substrack.downOctets += cocc->dataVolumeGPRSDownlink;
      retCode =  addSubstrack(&substrack);
      substrack.upOctets = 0;
      substrack.downOctets = 0;
      if( retCode)
      {
	snprintf(logStr,sizeof(logStr),"Error in addSubstrack:%d\n",retCode);
	debugLogging(DEBUG_CRITICAL,_CDR_DECODER_LOG_NAME_,logStr);
	return -3;
      }
    }
  }
  else
    retCode=-2;
  return retCode;
}
