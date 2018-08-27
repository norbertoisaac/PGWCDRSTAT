#include "cdrDecoder.h"
#include "string.h"
#include "stdlib.h"
#include "tc-misc.h"
#include "tc-mydb.h"
#include "time.h"
#include "tc-mydebug.h"
/*Compare ULI*/
int cmpUli(struct _userLocationInformation uliA, struct _userLocationInformation uliB)
{
  int retCode=0;
  if( uliA.ci > uliB.ci) retCode = 1;
  else if( uliA.ci < uliB.ci) retCode = -1;
  else if( uliA.eci > uliB.eci) retCode = 1;
  else if( uliA.eci < uliB.eci) retCode = -1;
  else if( uliA.mcc > uliB.mcc) retCode = 1;
  else if( uliA.mcc < uliB.mcc) retCode = -1;
  else if( uliA.mnc > uliB.mnc) retCode = 1;
  else if( uliA.mnc < uliB.mnc) retCode = -1;
  else if( uliA.eciMcc > uliB.eciMcc) retCode = 1;
  else if( uliA.eciMcc < uliB.eciMcc) retCode = -1;
  else if( uliA.eciMnc > uliB.eciMnc) retCode = 1;
  else if( uliA.eciMnc < uliB.eciMnc) retCode = -1;
  else if( uliA.lac > uliB.lac) retCode = 1;
  else if( uliA.lac < uliB.lac) retCode = -1;
  return retCode;
}
/*Compare time tm*/
int cmpTimeTm( struct tm tmA, struct tm tmB)
{
  int retCode=0;
  if( tmA.tm_min > tmB.tm_min) retCode = 1;
  else if( tmA.tm_min < tmB.tm_min) retCode = -1;
  else if( tmA.tm_sec > tmB.tm_sec) retCode = 1;
  else if( tmA.tm_sec < tmB.tm_sec) retCode = -1;
  else if( tmA.tm_hour > tmB.tm_hour) retCode = 1;
  else if( tmA.tm_hour < tmB.tm_hour) retCode = -1;
  else if( tmA.tm_yday > tmB.tm_yday) retCode = 1;
  else if( tmA.tm_yday < tmB.tm_yday) retCode = -1;
  else if( tmA.tm_year > tmB.tm_year) retCode = 1;
  else if( tmA.tm_year < tmB.tm_year) retCode = -1;
  return retCode;
}
/*Compare bettween two pgwRecordCOCC struct for DB routines*/
int cmpPgwStatCOCC( void *p1, void *p2)
{
  int result=0;
  struct _pgwRecordCOCC *pRCOCCa=p1;
  struct _pgwRecordCOCC *pRCOCCb=p2;
  /*Compare user location information*/
  result = cmpUli( pRCOCCa->uLI, pRCOCCb->uLI);
  if(!result)
  {
    result = memcmp(pRCOCCa->snaddr.s6_addr,pRCOCCb->snaddr.s6_addr,16);
    if(!result)
    {
      result = cmpTimeTm( pRCOCCa->changeTime, pRCOCCb->changeTime);
      if(!result)
      {
        result = strcmp(pRCOCCa->apnni,pRCOCCb->apnni);
	if(!result)
	{
	  result = memcmp(pRCOCCa->gwaddr.s6_addr,pRCOCCb->gwaddr.s6_addr,16);
	  if(!result)
	  {
	    if(pRCOCCa->withtraffic > pRCOCCb->withtraffic) result=1;
	    else if(pRCOCCa->withtraffic < pRCOCCb->withtraffic) result=-1;
	    else if(pRCOCCa->cc > pRCOCCb->cc) result=1;
	    else if(pRCOCCa->cc < pRCOCCb->cc) result=-1;
	    else if(pRCOCCa->sntype > pRCOCCb->sntype) result=1;
	    else if(pRCOCCa->sntype < pRCOCCb->sntype) result=-1;
	    else if(pRCOCCa->rATType > pRCOCCb->rATType) result=1;
	    else if(pRCOCCa->rATType < pRCOCCb->rATType) result=-1;
	    else if(pRCOCCa->imssignalingcontext > pRCOCCb->imssignalingcontext) result=1;
	    else if(pRCOCCa->imssignalingcontext < pRCOCCb->imssignalingcontext) result=-1;
	    else if(pRCOCCa->chchselectionmode > pRCOCCb->chchselectionmode) result=1;
	    else if(pRCOCCa->chchselectionmode < pRCOCCb->chchselectionmode) result=-1;
	    else if(pRCOCCa->apnselectionmode > pRCOCCb->apnselectionmode) result=1;
	    else if(pRCOCCa->apnselectionmode < pRCOCCb->apnselectionmode) result=-1;
	    else if(pRCOCCa->dynamicaddressflag > pRCOCCb->dynamicaddressflag) result=1;
	    else if(pRCOCCa->dynamicaddressflag < pRCOCCb->dynamicaddressflag) result=-1;
	    else if(pRCOCCa->snmcc > pRCOCCb->snmcc) result=1;
	    else if(pRCOCCa->snmcc < pRCOCCb->snmcc) result=-1;
	    else if(pRCOCCa->snmnc > pRCOCCb->snmnc) result=1;
	    else if(pRCOCCa->snmnc < pRCOCCb->snmnc) result=-1;
	    else if(pRCOCCa->gwmcc > pRCOCCb->gwmcc) result=1;
	    else if(pRCOCCa->gwmcc < pRCOCCb->gwmcc) result=-1;
	    else if(pRCOCCa->gwmnc > pRCOCCb->gwmnc) result=1;
	    else if(pRCOCCa->gwmnc < pRCOCCb->gwmnc) result=-1;
	  }
        }
      }
    }
  }
  return result;
  //if( !result)
  //{
  //  /*Compare change time*/
  //  result = cmpTimeTm( pRCOCCa->changeTime, pRCOCCb->changeTime);
  //  if( !result)
  //  {
  //    /*Compare serving node address*/
  //    result = memcmp(&pRCOCCa->snAddr,&pRCOCCb->snAddr,sizeof(pRCOCCa->snAddr));
  //    if( !result)
  //    {
  //      /*Compare gateway node address*/
  //      result = memcmp(&pRCOCCa->gwAddr,&pRCOCCb->gwAddr,sizeof(pRCOCCa->gwAddr));
  //      if( !result)
  //      {
  //        /*Compare APN*/
  //        result = strcmp( pRCOCCa->apn, pRCOCCb->apn);
  //        if( !result)
  //        {
  //          /*Compare rat type*/
  //          if( pRCOCCa->rATType > pRCOCCb->rATType) result = 1;
  //          else if( pRCOCCa->rATType < pRCOCCb->rATType) result = -1;
  //          else
  //          {
  //            /*Compare charging characteristics*/
  //            if( pRCOCCa->chargingCharacteristic > pRCOCCb->chargingCharacteristic) return 1;
  //            else if( pRCOCCa->chargingCharacteristic < pRCOCCb->chargingCharacteristic) return -1;
  //            /*Compare serving node type*/
  //            else
  //            {
  //              if( pRCOCCa->snType > pRCOCCb->snType) return 1;
  //              else if( pRCOCCa->snType < pRCOCCb->snType) return -1;
  //            }
  //            ///*Compare node id*/
  //            //result = strcmp( pRCOCCa->nodeId, pRCOCCb->nodeId);
  //          }
  //        }
  //      }
  //    }
  //  }
  //}
  //return result;
  //    strcpy(pgwRecordCOCC->apnni,pgwRecord->accessPointNameNI);
  //    pgwRecordCOCC->gwaddr = pgwRecord->p_GWAddress;
  //    pgwRecordCOCC->gwmcc = pgwRecord->p_GWPLMNIdentifier.mcc;
  //    pgwRecordCOCC->gwmnc = pgwRecord->p_GWPLMNIdentifier.mnc;
  //    pgwRecordCOCC->snaddr = pgwRecord->servingNodeAddress;
  //    pgwRecordCOCC->sntype = pgwRecord->servingNodeType;
  //    pgwRecordCOCC->snmcc = pgwRecord->servingNodePLMNIdentifier.mcc;
  //    pgwRecordCOCC->snmnc = pgwRecord->servingNodePLMNIdentifier.mnc;
  //    pgwRecordCOCC->uLI = cocc->userLocationInformation;
  //    pgwRecordCOCC->rATType = pgwRecord->rATType;
  //    pgwRecordCOCC->cc = pgwRecord->chargingCharacteristics;
  //    pgwRecordCOCC->apnselectionmode = pgwRecord->apnSelectionMode;
  //    pgwRecordCOCC->chchselectionmode = pgwRecord->chChSelectionMode;
  //    pgwRecordCOCC->dynamicaddressflag = pgwRecord->dynamicAddressFlag;
  //    pgwRecordCOCC->imssignalingcontext = pgwRecord->iMSsignalingContext;
  //    //pgwRecordCOCC->pgwcdrcmbid = cdr_pgwcdrcmb_id;
  //    pgwRecordCOCC->changeTime = timeTM;
  //    if(cocc->dataVolumeGPRSUplink || cocc->dataVolumeGPRSDownlink)
  //      pgwRecordCOCC->withtraffic = 1;
}
/*Compare bettween two pgwRecordCOSC struct for DB routines*/
int cmpPgwStatCOSC( void *p1, void *p2)
{
  int result=0;
  struct _pgwRecordCOSC *pRCOSCa=p1;
  struct _pgwRecordCOSC *pRCOSCb=p2;
  /*Compare serving node*/
  result = memcmp(pRCOSCa->snaddr.s6_addr,pRCOSCb->snaddr.s6_addr, sizeof(pRCOSCa->snaddr.s6_addr));
  if(!result)
  {
    /*Compare service identifier*/
    if( pRCOSCa->serviceIdentifier > pRCOSCb->serviceIdentifier) result = 1;
    else if( pRCOSCa->serviceIdentifier < pRCOSCb->serviceIdentifier) result = -1;
    else
    {
      /*Compare result code*/
      if( pRCOSCa->resultCode > pRCOSCb->resultCode) result = 1;
      else if( pRCOSCa->resultCode < pRCOSCb->resultCode) result = -1;
      else
      {
	/*Compare time of report*/
	result = cmpTimeTm( pRCOSCa->timeOfReport, pRCOSCb->timeOfReport);
	if ( !result)
	{
	  /*Compare failure handling continue*/
	  if( pRCOSCa->failureHandlingContinue > pRCOSCb->failureHandlingContinue) result = 1;
	  else if( pRCOSCa->failureHandlingContinue < pRCOSCb->failureHandlingContinue) result = -1;
	    /*Compare rating group*/
	  else if( pRCOSCa->ratingGroup > pRCOSCb->ratingGroup) result = 1;
	  else if( pRCOSCa->ratingGroup < pRCOSCb->ratingGroup) result = -1;
	  else if(pRCOSCa->rATType>pRCOSCb->rATType) result=1;
	  else if(pRCOSCa->rATType<pRCOSCb->rATType) result=-1;
	  else if(pRCOSCa->snmcc>pRCOSCb->snmcc) result=1;
	  else if(pRCOSCa->snmcc<pRCOSCb->snmcc) result=-1;
	  else if(pRCOSCa->snmnc>pRCOSCb->snmnc) result=1;
	  else if(pRCOSCa->snmnc<pRCOSCb->snmnc) result=-1;
	  else if(pRCOSCa->gwmcc>pRCOSCb->gwmcc) result=1;
	  else if(pRCOSCa->gwmcc<pRCOSCb->gwmcc) result=-1;
	  else if(pRCOSCa->gwmnc>pRCOSCb->gwmnc) result=1;
	  else if(pRCOSCa->gwmnc<pRCOSCb->gwmnc) result=-1;
	  else if(pRCOSCa->sntype>pRCOSCb->sntype) result=1;
	  else if(pRCOSCa->sntype<pRCOSCb->sntype) result=-1;
	  else if(pRCOSCa->cc>pRCOSCb->cc) result=1;
	  else if(pRCOSCa->cc<pRCOSCb->cc) result=-1;
	  else
	  {
	    result = strcmp(pRCOSCa->apnni,pRCOSCb->apnni);
	    if(!result)
	    {
	      result = strcmp(pRCOSCa->crbn,pRCOSCb->crbn);
	      if(!result)
	      {
		result = memcmp(pRCOSCa->gwaddr.s6_addr,pRCOSCb->gwaddr.s6_addr, sizeof(pRCOSCa->gwaddr.s6_addr));
	      }
	    }
	  }
	}
      }
    }
  }
  return result;
}
/*Compare between pdpStat struct*/
int cmpPdpStat(void *a, void *b)
{
  int retCode=0;
  struct _pdpStat *pdpStatA=a;
  struct _pdpStat *pdpStatB=b;
  /*serving node address*/
  retCode = memcmp(pdpStatA->servingNodeAddr.s6_addr,pdpStatB->servingNodeAddr.s6_addr,16);
  if( !retCode)
  {
    /*APN*/
    retCode = strcmp(pdpStatA->apn,pdpStatB->apn);
    if( !retCode)
    {
      /*Timestamp*/
      if( pdpStatA->tst>pdpStatB->tst) retCode=1;
      else if( pdpStatA->tst<pdpStatB->tst) retCode=-1;
      else
      {
        /*GW address*/
        retCode = memcmp(pdpStatA->gwAddr.s6_addr,pdpStatB->gwAddr.s6_addr,16);
	if(!retCode)
	{
	  /*RAT type*/
	  if(pdpStatA->rATType>pdpStatB->rATType) retCode=1;
	  else if(pdpStatA->rATType<pdpStatB->rATType) retCode=-1;
	}
      }
    }
  }
  return retCode;
}
int addPdpStat( struct _pdpStat *pdpStat, int pdpStatDB)
{
  int retCode=0;
  struct _pdpStat *pdpStatA=NULL;
  struct _pdpStat *pdpStatB=NULL;
  /*Insert/update pdpStart*/
  if( pdpStat->pdpStart)
  {
    pdpStatA = calloc(1,sizeof(struct _pdpStat));
    *pdpStatA = *pdpStat;
    /*Search the same struct*/
    pdpStatB = myDB_insertRow( pdpStatDB, pdpStatA);
    /*Update the row*/
    if( pdpStatB)
    {
      if( pdpStatB != pdpStatA )
      {
	pdpStatB->pdpStart += pdpStatA->pdpStart;
      }
    }
    else
    {
      pdpStatA = NULL;
    }
  }
  /*Insert/update pdpStop*/
  if( pdpStat->pdpStop)
  {
    if( !pdpStatA)
    {
      pdpStatA = calloc(1,sizeof(struct _pdpStat));
      *pdpStatA = *pdpStat;
    }
    /*Search the same struct*/
    pdpStatB = myDB_insertRow( pdpStatDB, pdpStatA);
    /*Update the row*/
    if( pdpStatB)
    {
      if( pdpStatB != pdpStatA )
      {
	pdpStatB->pdpStop += pdpStatA->pdpStop;
      }
    }
    else
    {
      pdpStatA = NULL;
    }
  }
  /*Insert/update pdpConcurrent*/
  if( pdpStat->pdpConcurrent)
  {
    if( !pdpStatA)
    {
      pdpStatA = calloc(1,sizeof(struct _pdpStat));
      *pdpStatA = *pdpStat;
    }
    /*Search the same struct*/
    pdpStatB = myDB_insertRow( pdpStatDB, pdpStatA);
    /*Update the row*/
    if( pdpStatB)
    {
      if( pdpStatB != pdpStatA )
      {
	pdpStatB->pdpConcurrent += pdpStatA->pdpConcurrent;
      }
    }
    else
    {
      pdpStatA = NULL;
    }
  }
  if( pdpStat->pdpUpdate)
  {
  }
  if( pdpStatA)
    free(pdpStatA);
  return retCode;
}
unsigned int statTimeDiv=60;
unsigned int pdpStatTimeDiv=300;
int cdrPGWRecordStat( struct _cdrPGWRecordStatParams *statParams)
{
  int retCode=0;
  struct _pgwCdrStatRes pgwCdrStatRes = {0};
  unsigned int i, duration;
  struct _pgwRecordCOCC *pgwRecordCOCC=NULL;
  struct _pgwRecordCOCC *pgwRecordCOCCA=NULL;
  struct _pgwRecordCOCC *pgwRecordCOCCB=NULL;
  struct _pgwRecordCOSC *pgwRecordCOSC=NULL;
  struct _pgwRecordCOSC *pgwRecordCOSCA=NULL;
  struct _pgwRecordCOSC *pgwRecordCOSCB=NULL;
  struct _pdpStat pdpStat={0};
  struct _ChangeOfCharCondition *cocc=NULL;
  struct _ChangeOfServiceCondition *cosc=NULL;
  struct _userLocationInformation localUli = {{0}};
  struct _cdrCi *cdr_ci = NULL, *cdr_ci_B = NULL;
  struct _losdTables *losdTablesA=NULL, *losdTablesB=NULL;
  struct _lotvTables *lotvTablesA=NULL;
  time_t t, lastT, currentT, timeOfFirstUsage, timeOfLastUsage;
  struct tm timeTM;
  struct _pgwRecord *pgwRecord = statParams->pgwRecord;
  int coccDB = statParams->coccDB;
  int coscDB = statParams->coscDB;
  int pdpStatDB = statParams->pdpStatDB;
  int ciDB = statParams->ciDB;
  int lotvTablesDB = statParams->lotvTablesDB;
  int losdTablesDB = statParams->losdTablesDB;
  //uint64_t cdr_pgwcdrcmb_id;
  uint32_t *cdrCi_id = statParams->cdrCi_id, ci_id=0;
  uint8_t first_withtraffic=0;
  struct _cdr_pgwcdrcmb *cdr_pgwcdrcmb_A=NULL, *cdr_pgwcdrcmb_B=NULL;
  /*PGW usage record statistic resume*/
  t = mktime(&pgwRecord->recordOpeningTime);
  lastT = t;
  pdpStat.tst = t - (t%pdpStatTimeDiv);
  strcpy(pdpStat.apn,pgwRecord->accessPointNameNI);
  memcpy(pdpStat.gwAddr.s6_addr, pgwRecord->p_GWAddress.s6_addr, 16);
  memcpy(pdpStat.gwAddr.s6_addr, pgwRecord->p_GWAddress.s6_addr, 16);
  memcpy(pdpStat.servingNodeAddr.s6_addr, pgwRecord->servingNodeAddress.s6_addr, 16);
  pdpStat.rATType = pgwRecord->rATType;
  pdpStat.mcc = pgwRecord->servingNodePLMNIdentifier.mcc;
  pdpStat.mnc = pgwRecord->servingNodePLMNIdentifier.mnc;
  pdpStat.servingNodeType = pgwRecord->servingNodeType;
  /*Start condition*/
  if( !pgwRecord->recordSequenceNumber)
  {
    pdpStat.pdpStart = 1;
    //update/insert start
    pdpStat.pdpConcurrent = 0;
    pdpStat.pdpStop = 0;
    addPdpStat( &pdpStat, pdpStatDB);
  }
  /*Councurrent sessions*/
  duration = pgwRecord->duration;
  while(duration>0)
  {
    if((pdpStat.tst+pdpStatTimeDiv)>=(t+duration))
    {
      pdpStat.pdpConcurrent = (float)duration/(float)pdpStatTimeDiv;
      //update/insert pdpconcurrent
      pdpStat.pdpStart = 0;
      pdpStat.pdpStop = 0;
      addPdpStat( &pdpStat, pdpStatDB);
      duration = 0;
    }
    else
    {
      pdpStat.pdpConcurrent = (float)(pdpStat.tst+pdpStatTimeDiv-t)/(float)pdpStatTimeDiv;
      //update/insert pdpconcurrent
      pdpStat.pdpStart = 0;
      pdpStat.pdpStop = 0;
      addPdpStat( &pdpStat, pdpStatDB);
      duration = (duration+t) - (pdpStat.tst+pdpStatTimeDiv);
      if( duration>0)
      {
	pdpStat.tst = pdpStat.tst+pdpStatTimeDiv;
	t = pdpStat.tst;
      }
    }
  }
  /*Stop condition*/
  if( pgwRecord->causeForRecClosing <= 15)
  {
    t = mktime(&pgwRecord->recordOpeningTime)+pgwRecord->duration;
    pdpStat.tst = t - (t%pdpStatTimeDiv);
    pdpStat.pdpStop = 1;
    pdpStat.pdpStart = 0;
    pdpStat.pdpConcurrent = 0;
    addPdpStat( &pdpStat, pdpStatDB);
  }
  if(pgwRecord->listOfTrafficVolumesCount)
  {
    
    /*listOfTrafficVolume statistics*/
    for(i=0,cocc=&pgwRecord->listOfTrafficVolumes; cocc&&(i<pgwRecord->listOfTrafficVolumesCount); cocc=cocc->next, i++)
    {
      t = mktime(&cocc->changeTime);
      currentT = t;
      t = t-(t%statTimeDiv);
      localtime_r( &t, &timeTM);
      /*ULI*/
      if( !cmpUli( cocc->userLocationInformation, localUli))
      {
	cocc->userLocationInformation = pgwRecord->userLocationInformation;
        //cdr_pgwcdrcmb_id = statParams->cdr_pgwcdrcmb->id;
      }
      else if( cmpUli( cocc->userLocationInformation, pgwRecord->userLocationInformation))
      {
	/*Cell identity DB*/
	if(!cdr_ci)
	  cdr_ci = calloc(1,sizeof(struct _cdrCi));
	cdr_ci->uli = cocc->userLocationInformation;
	cdr_ci->ratType = pgwRecord->rATType;
	cdr_ci_B = myDB_insertRow( ciDB, cdr_ci);
	if(!cdr_ci_B)
	{
	  cdr_ci->id = (*cdrCi_id)++;
	  ci_id = cdr_ci->id;
	  cdr_ci = NULL;
	}
	else
	{
	  ci_id = cdr_ci_B->id;
	}
	/*PGWCDR combination DB*/
	if(!cdr_pgwcdrcmb_A)
	  cdr_pgwcdrcmb_A = calloc(1,sizeof(struct _cdr_pgwcdrcmb));
	*cdr_pgwcdrcmb_A = *statParams->cdr_pgwcdrcmb;
	cdr_pgwcdrcmb_A->cdrCi_id = ci_id;
	cdr_pgwcdrcmb_A->tst_tm = cocc->changeTime;
	cdr_pgwcdrcmb_B = myDB_insertRow( statParams->pgwCdrCmbDB, cdr_pgwcdrcmb_A);
	if(!cdr_pgwcdrcmb_B)
	{
	  cdr_pgwcdrcmb_A->id = (*statParams->pgwRecordTablesId)++;
	  //cdr_pgwcdrcmb_id = cdr_pgwcdrcmb_A->id;
	  cdr_pgwcdrcmb_A = NULL;
	}
	else
	{
	  //cdr_pgwcdrcmb_id = cdr_pgwcdrcmb_B->id;
	}
      }
      else
      {
        //cdr_pgwcdrcmb_id = statParams->cdr_pgwcdrcmb->id;
      }
      if( !pgwRecordCOCC)
      {
	pgwRecordCOCC = calloc(1,sizeof(struct _pgwRecordCOCC));
	if( !pgwRecordCOCC)
	{
	  retCode = -2;
	  break;
	}
      }
      else
      {
	bzero(pgwRecordCOCC,sizeof(struct _pgwRecordCOCC));
      }
      //pgwRecordCOCCB = pgwRecordCOCC;
      /*Populate the struct*/
      strcpy(pgwRecordCOCC->apnni,pgwRecord->accessPointNameNI);
      pgwRecordCOCC->gwaddr = pgwRecord->p_GWAddress;
      pgwRecordCOCC->gwmcc = pgwRecord->p_GWPLMNIdentifier.mcc;
      pgwRecordCOCC->gwmnc = pgwRecord->p_GWPLMNIdentifier.mnc;
      pgwRecordCOCC->snaddr = pgwRecord->servingNodeAddress;
      pgwRecordCOCC->sntype = pgwRecord->servingNodeType;
      pgwRecordCOCC->snmcc = pgwRecord->servingNodePLMNIdentifier.mcc;
      pgwRecordCOCC->snmnc = pgwRecord->servingNodePLMNIdentifier.mnc;
      pgwRecordCOCC->uLI = cocc->userLocationInformation;
      pgwRecordCOCC->rATType = pgwRecord->rATType;
      pgwRecordCOCC->cc = pgwRecord->chargingCharacteristics;
      pgwRecordCOCC->apnselectionmode = pgwRecord->apnSelectionMode;
      pgwRecordCOCC->chchselectionmode = pgwRecord->chChSelectionMode;
      pgwRecordCOCC->dynamicaddressflag = pgwRecord->dynamicAddressFlag;
      pgwRecordCOCC->imssignalingcontext = pgwRecord->iMSsignalingContext;
      pgwRecordCOCC->changeTime = timeTM;
      if(cocc->dataVolumeGPRSUplink || cocc->dataVolumeGPRSDownlink)
	pgwRecordCOCC->withtraffic = 1;
      else
	pgwRecordCOCC->withtraffic = 0;
      //pgwRecordCOCC->pgwcdrcmbid = cdr_pgwcdrcmb_id;
      //pgwRecordCOCC->snAddr = pgwRecord->servingNodeAddress;
      //pgwRecordCOCC->snType = pgwRecord->servingNodeType;
      //pgwRecordCOCC->chargingCharacteristic = pgwRecord->chargingCharacteristics;
      /*Search the same struct*/
      pgwRecordCOCCA = myDB_insertRow( coccDB, pgwRecordCOCC);
      /*Update the row*/
      if( pgwRecordCOCCA)
      {
	if( pgwRecordCOCCA != pgwRecordCOCC )
	{
	  pgwRecordCOCCB = pgwRecordCOCCA;
	}
	else
	{
	  retCode = -3;
	  break;
	}
      }
      else
      {
	pgwCdrStatRes.totalCoccInsert++;
	pgwRecordCOCCB = pgwRecordCOCC;
	pgwRecordCOCC = NULL;
      }
      if(pgwRecordCOCCB->withtraffic)
      {
	pgwRecordCOCCB->dataVolumeGPRSUplink += cocc->dataVolumeGPRSUplink;
	pgwRecordCOCCB->dataVolumeGPRSDownlink += cocc->dataVolumeGPRSDownlink;
      }
      if( cocc->changeCondition == ChangeCondition_qoSChange) pgwRecordCOCCB->qoSChange++;
      if( cocc->changeCondition == ChangeCondition_tariffTime) pgwRecordCOCCB->tariffTime++;
      if( cocc->changeCondition == ChangeCondition_recordClosure) pgwRecordCOCCB->recordClosure++;
      if( cocc->changeCondition == ChangeCondition_cGI_SAICHange) pgwRecordCOCCB->cGI_SAICHange++;
      if( cocc->changeCondition == ChangeCondition_rAIChange) pgwRecordCOCCB->rAIChange++;
      if( cocc->changeCondition == ChangeCondition_dT_Establishment) pgwRecordCOCCB->dT_Establishment++;
      if( cocc->changeCondition == ChangeCondition_dT_Removal) pgwRecordCOCCB->dT_Removal++;
      if( cocc->changeCondition == ChangeCondition_eCGIChange) pgwRecordCOCCB->eCGIChange++;
      if( cocc->changeCondition == ChangeCondition_tAIChange) pgwRecordCOCCB->tAIChange++;
      if( cocc->changeCondition == ChangeCondition_userLocationChange) pgwRecordCOCCB->userLocationChange++;
      /*Start condition*/
      if(!i)
      {
        first_withtraffic = pgwRecordCOCCB->withtraffic;
      }
      /*Concurrent part*/
      pgwRecordCOCCB->pdpconcurrent += (float)(currentT-lastT)/(float)statTimeDiv;
      lastT = currentT;
      /*Stop condition*/
      if(i == (pgwRecord->listOfTrafficVolumesCount -1))
      {
        if( pgwRecord->causeForRecClosing == causeForRecClosing_normalRelease) pgwRecordCOCCB->pdpNormalStop++;
        else if( pgwRecord->causeForRecClosing == causeForRecClosing_abnormalRelease) pgwRecordCOCCB->pdpAbnormalStop++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_servingNodeChange) pgwRecordCOCCB->servingNodeChange++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_managementIntervention) pgwRecordCOCCB->managementIntervention++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_intraSGSNIntersystemChange) pgwRecordCOCCB->intraSGSNIntersystemChange++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_rATChange) pgwRecordCOCCB->rATChange++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_mSTimeZoneChange) pgwRecordCOCCB->mSTimeZoneChange++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_sGSNPLMNIDChange) pgwRecordCOCCB->sGSNPLMNIDChange++;
      //  else if( pgwRecord->causeForRecClosing == causeForRecClosing_unauthorizedRequestingNetwork) pgwRecordCOCCB->unauthorizedRequestingNetwork++;
      }
      /*Save the table name*/
      if( !lotvTablesA)
	lotvTablesA = calloc(1,sizeof(struct _lotvTables));
      strftime(lotvTablesA->name,lotvTableLen,"%Y%m%d%H%M",&timeTM);
      lotvTablesA->tm = timeTM;
      if(!myDB_insertRow(lotvTablesDB,lotvTablesA))
	lotvTablesA = NULL;
    }
  }
  /* without traffic compute*/
  else
  {
      t = mktime(&pgwRecord->recordOpeningTime);
      t = t-(t%statTimeDiv);
      localtime_r( &t, &timeTM);
      if( !pgwRecordCOCC)
      {
	pgwRecordCOCC = calloc(1,sizeof(struct _pgwRecordCOCC));
	if( !pgwRecordCOCC)
	{
	  retCode = -2;
	}
      }
      else
      {
	bzero(pgwRecordCOCC,sizeof(struct _pgwRecordCOCC));
      }
      pgwRecordCOCCB = pgwRecordCOCC;
      /*Populate the struct*/
      pgwRecordCOCC->pgwcdrcmbid = statParams->cdr_pgwcdrcmb->id;
      pgwRecordCOCC->withtraffic = 0;
      pgwRecordCOCC->changeTime = timeTM;
      /*Search the same struct*/
      pgwRecordCOCCA = myDB_insertRow( coccDB, pgwRecordCOCC);
      /*Update the row*/
      if( pgwRecordCOCCA)
      {
	if( pgwRecordCOCCA != pgwRecordCOCC )
	{
	  pgwRecordCOCCB = pgwRecordCOCCA;
	}
	else
	{
	  retCode = -3;
	}
      }
      else
      {
	pgwRecordCOCC = NULL;
	pgwCdrStatRes.totalCoccInsert++;
      }
      /*Concurrent part*/
      pgwRecordCOCCB->pdpconcurrent += (float)(pgwRecord->duration)/(float)statTimeDiv;
      /*Save the table name*/
      if( !lotvTablesA)
	lotvTablesA = calloc(1,sizeof(struct _lotvTables));
      strftime(lotvTablesA->name,lotvTableLen,"%Y%m%d%H%M",&timeTM);
      lotvTablesA->tm = timeTM;
      if(!myDB_insertRow(lotvTablesDB,lotvTablesA))
	lotvTablesA = NULL;
  }
  /*Start condition*/
  if( !pgwRecord->recordSequenceNumber)
  {
      if( !pgwRecordCOCC)
      {
        pgwRecordCOCC = calloc(1,sizeof(struct _pgwRecordCOCC));
        if( !pgwRecordCOCC)
        {
          retCode = -2;
        }
      }
      else
      {
        bzero(pgwRecordCOCC,sizeof(struct _pgwRecordCOCC));
      }
      /*Populate the struct*/
      t = mktime(&pgwRecord->recordOpeningTime);
      t = t-(t%statTimeDiv);
      localtime_r( &t, &timeTM);
      strcpy(pgwRecordCOCC->apnni,pgwRecord->accessPointNameNI);
      pgwRecordCOCC->gwaddr = pgwRecord->p_GWAddress;
      pgwRecordCOCC->gwmcc = pgwRecord->p_GWPLMNIdentifier.mcc;
      pgwRecordCOCC->gwmnc = pgwRecord->p_GWPLMNIdentifier.mnc;
      pgwRecordCOCC->snaddr = pgwRecord->servingNodeAddress;
      pgwRecordCOCC->sntype = pgwRecord->servingNodeType;
      pgwRecordCOCC->snmcc = pgwRecord->servingNodePLMNIdentifier.mcc;
      pgwRecordCOCC->snmnc = pgwRecord->servingNodePLMNIdentifier.mnc;
      pgwRecordCOCC->uLI =  pgwRecord->userLocationInformation;
      pgwRecordCOCC->rATType = pgwRecord->rATType;
      pgwRecordCOCC->cc = pgwRecord->chargingCharacteristics;
      pgwRecordCOCC->apnselectionmode = pgwRecord->apnSelectionMode;
      pgwRecordCOCC->chchselectionmode = pgwRecord->chChSelectionMode;
      pgwRecordCOCC->dynamicaddressflag = pgwRecord->dynamicAddressFlag;
      pgwRecordCOCC->imssignalingcontext = pgwRecord->iMSsignalingContext;
      pgwRecordCOCC->changeTime = timeTM;
      pgwRecordCOCC->withtraffic = first_withtraffic;
      /*Search the same struct*/
      pgwRecordCOCCA = myDB_insertRow( coccDB, pgwRecordCOCC);
      /*Update the row*/
      if( pgwRecordCOCCA)
      {
        if( pgwRecordCOCCA != pgwRecordCOCC )
        {
          pgwRecordCOCCB = pgwRecordCOCCA;
        }
        else
        {
          retCode = -3;
        }
      }
      else
      {
	pgwRecordCOCCB = pgwRecordCOCC;
        pgwRecordCOCC = NULL;
        pgwCdrStatRes.totalCoccInsert++;
      }
      /*Concurrent part*/
      pgwRecordCOCCB->pdpStart++;
      /*Save the table name*/
      if( !lotvTablesA)
	lotvTablesA = calloc(1,sizeof(struct _lotvTables));
      strftime(lotvTablesA->name,lotvTableLen,"%Y%m%d%H%M",&timeTM);
      lotvTablesA->tm = timeTM;
      if(!myDB_insertRow(lotvTablesDB,lotvTablesA))
	lotvTablesA = NULL;
  }
  /*listOfServiceData statistics*/
  for(i=0,cosc=&pgwRecord->listOfServiceData; cosc&&(i<pgwRecord->listOfServiceDataCount); cosc=cosc->next, i++)
  {
    t = mktime(&cosc->timeOfReport);
    t = t-(t%statTimeDiv);
    localtime_r( &t, &timeTM);
    /*ULI*/
    if( !cmpUli( cosc->userLocationInformation, localUli))
    {
      cosc->userLocationInformation = pgwRecord->userLocationInformation;
      //cdr_pgwcdrcmb_id = statParams->cdr_pgwcdrcmb->id;
    }
    else if( cmpUli( cosc->userLocationInformation, pgwRecord->userLocationInformation))
    {
      /*Cell identity DB*/
      if(!cdr_ci)
        cdr_ci = calloc(1,sizeof(struct _cdrCi));
      cdr_ci->uli = cosc->userLocationInformation;
      cdr_ci->ratType = pgwRecord->rATType;
      cdr_ci_B = myDB_insertRow( ciDB, cdr_ci);
      if(!cdr_ci_B)
      {
        cdr_ci->id = (*cdrCi_id)++;
        ci_id = cdr_ci->id;
        cdr_ci = NULL;
      }
      else
      {
        ci_id = cdr_ci_B->id;
      }
      /*PGWCDR combination DB*/
      if(!cdr_pgwcdrcmb_A)
        cdr_pgwcdrcmb_A = calloc(1,sizeof(struct _cdr_pgwcdrcmb));
      *cdr_pgwcdrcmb_A = *statParams->cdr_pgwcdrcmb;
      cdr_pgwcdrcmb_A->cdrCi_id = ci_id;
      cdr_pgwcdrcmb_A->tst_tm = cosc->timeOfReport;
      cdr_pgwcdrcmb_B = myDB_insertRow( statParams->pgwCdrCmbDB, cdr_pgwcdrcmb_A);
      if(!cdr_pgwcdrcmb_B)
      {
        cdr_pgwcdrcmb_A->id = (*statParams->pgwRecordTablesId)++;
        //cdr_pgwcdrcmb_id = cdr_pgwcdrcmb_A->id;
        cdr_pgwcdrcmb_A = NULL;
      }
      else
      {
        //cdr_pgwcdrcmb_id = cdr_pgwcdrcmb_B->id;
      }
    }
    else
    {
      //cdr_pgwcdrcmb_id = statParams->cdr_pgwcdrcmb->id;
    }
    if( !pgwRecordCOSC)
    {
      pgwRecordCOSC = calloc(1,sizeof(struct _pgwRecordCOSC));
      if( !pgwRecordCOSC)
      {
	retCode = -4;
	break;
      }
    }
    else
    {
      bzero(pgwRecordCOSC,sizeof(struct _pgwRecordCOSC));
    }
    pgwRecordCOSCB = pgwRecordCOSC;
    /*Populate the struct*/
    strcpy(pgwRecordCOSC->apnni,pgwRecord->accessPointNameNI);
    pgwRecordCOSC->rATType = pgwRecord->rATType;
    pgwRecordCOSC->gwaddr = pgwRecord->p_GWAddress;
    pgwRecordCOSC->gwmcc = pgwRecord->p_GWPLMNIdentifier.mcc;
    pgwRecordCOSC->gwmnc = pgwRecord->p_GWPLMNIdentifier.mnc;
    pgwRecordCOSC->snaddr = pgwRecord->servingNodeAddress;
    pgwRecordCOSC->sntype = pgwRecord->servingNodeType;
    pgwRecordCOSC->snmcc = pgwRecord->servingNodePLMNIdentifier.mcc;
    pgwRecordCOSC->snmnc = pgwRecord->servingNodePLMNIdentifier.mnc;
    pgwRecordCOSC->cc = pgwRecord->chargingCharacteristics;
    strncpy(pgwRecordCOSC->crbn,cosc->chargingRuleBaseName,sizeof(pgwRecordCOSC->crbn));
    //pgwRecordCOSC->serviceConditionChange = cosc->serviceConditionChange;
    //pgwRecordCOSC->uLI = cosc->userLocationInformation;
    //pgwRecordCOSC->pgwcdrcmbid = cdr_pgwcdrcmb_id;
    //pgwRecordCOSC->pgwcdrcmbid = statParams->cdr_pgwcdrcmb->id;
    pgwRecordCOSC->ratingGroup = cosc->ratingGroup;
    pgwRecordCOSC->resultCode = cosc->resultCode;
    //pgwRecordCOSC->datavolumeFBCUplink = cosc->datavolumeFBCUplink;
    //pgwRecordCOSC->datavolumeFBCDownlink = cosc->datavolumeFBCDownlink;
    //pgwRecordCOSC->timeUsage = cosc->timeUsage;
    pgwRecordCOSC->timeOfReport = timeTM;
    pgwRecordCOSC->failureHandlingContinue = cosc->failureHandlingContinue;
    pgwRecordCOSC->serviceIdentifier = cosc->serviceIdentifier;
    //pgwRecordCOSC->sessions = 1;
    
    ///*For test*/
    //if((cosc->ratingGroup==4)&&(cosc->serviceIdentifier==5005)&&(cosc->resultCode==4011))
    //{
    //  char msgStr[1024];
    //  char timeStr[64];
    //  strftime(timeStr,sizeof(timeStr),"%Y%m%d%H%M%S",&cosc->timeOfReport);
    //  snprintf(msgStr,sizeof(msgStr),"time=%s,chargingId=%u,serviceconditionchange=%.2X-%.2X-%.2X-%.2X-%.2X",timeStr,pgwRecord->chargingID,cosc->serviceConditionChange.bytes[0],cosc->serviceConditionChange.bytes[1],cosc->serviceConditionChange.bytes[2],cosc->serviceConditionChange.bytes[3],cosc->serviceConditionChange.bytes[4]);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, msgStr);
    //}
    ///*For test*/
    //if(pgwRecord->chargingID==2338207362)
    //{
    //  char msgStr[1024];
    //  char timeStr[64];
    //  strftime(timeStr,sizeof(timeStr),"%Y%m%d%H%M%S",&cosc->timeOfReport);
    //  snprintf(msgStr,sizeof(msgStr),"time=%s,chargingId=%u,serviceconditionchange=%.2X-%.2X-%.2X-%.2X-%.2X",timeStr,pgwRecord->chargingID,cosc->serviceConditionChange.bytes[0],cosc->serviceConditionChange.bytes[1],cosc->serviceConditionChange.bytes[2],cosc->serviceConditionChange.bytes[3],cosc->serviceConditionChange.bytes[4]);
    //  debugLogging( DEBUG_CRITICAL, _CDR_DECODER_LOG_NAME_, msgStr);
    //}
    /*Search the same struct*/
    //retDBi = myDBIndex_r( coscDB, &localPgwRecordCOSC, cmpPgwStatCOSC, &retVal, MYDBINDEXOPTYPE_SEARCH);
    pgwRecordCOSCA = myDB_insertRow( coscDB, pgwRecordCOSC);
    /*Update the row*/
    if( pgwRecordCOSCA)
    {
      if( pgwRecordCOSCA != pgwRecordCOSC)
      {
	pgwRecordCOSCB = pgwRecordCOSCA;
      }
      else
      {
	retCode = -5;
	break;
      }
    }
    else
    {
      pgwCdrStatRes.totalCoscInsert++;
      pgwRecordCOSC = NULL;
    }
    pgwRecordCOSCB->hits++;
    pgwRecordCOSCB->datavolumeFBCUplink += cosc->datavolumeFBCUplink; 
    pgwRecordCOSCB->datavolumeFBCDownlink += cosc->datavolumeFBCDownlink;
    //pgwRecordCOSCB->timeUsage += cosc->timeUsage;
    //pgwRecordCOSCB->sessions++;
    if( cosc->serviceConditionChange.bits.qosChange) pgwRecordCOSCB->qoSChange++;
    if( cosc->serviceConditionChange.bits.sGSNChange) pgwRecordCOSCB->sGSNChange++;
    if( cosc->serviceConditionChange.bits.sGSNPLMNIDChange) pgwRecordCOSCB->sGSNPLMNIDChange++;
    if( cosc->serviceConditionChange.bits.tariffTimeSwitch) pgwRecordCOSCB->tariffTimeSwitch++;
    if( cosc->serviceConditionChange.bits.pDPContextRelease) pgwRecordCOSCB->pDPContextRelease++;
    if( cosc->serviceConditionChange.bits.rATChange) pgwRecordCOSCB->rATChange++;
    if( cosc->serviceConditionChange.bits.serviceIdledOut) pgwRecordCOSCB->serviceIdledOut++;
    if( cosc->serviceConditionChange.bits.configurationChange) pgwRecordCOSCB->configurationChange++;
    if( cosc->serviceConditionChange.bits.serviceStop) pgwRecordCOSCB->serviceStop++;
    if( cosc->serviceConditionChange.bits.dCCATimeThresholdReached) pgwRecordCOSCB->dCCATimeThresholdReached++;
    if( cosc->serviceConditionChange.bits.dCCAVolumeThresholdReached) pgwRecordCOSCB->dCCAVolumeThresholdReached++;
    if( cosc->serviceConditionChange.bits.dCCAServiceSpecificUnitThresholdReached) pgwRecordCOSCB->dCCAServiceSpecificUnitThresholdReached++;
    if( cosc->serviceConditionChange.bits.dCCATimeExhausted) pgwRecordCOSCB->dCCATimeExhausted++;
    if( cosc->serviceConditionChange.bits.dCCAVolumeExhausted) pgwRecordCOSCB->dCCAVolumeExhausted++;
    if( cosc->serviceConditionChange.bits.dCCAValidityTimeout) pgwRecordCOSCB->dCCAValidityTimeout++;
    if( cosc->serviceConditionChange.bits.dCCAReauthorisationRequest) pgwRecordCOSCB->dCCAReauthorisationRequest++;
    if( cosc->serviceConditionChange.bits.dCCAContinueOngoingSession) pgwRecordCOSCB->dCCAContinueOngoingSession++;
    if( cosc->serviceConditionChange.bits.dCCARetryAndTerminateOngoingSession) pgwRecordCOSCB->dCCARetryAndTerminateOngoingSession++;
    if( cosc->serviceConditionChange.bits.dCCATerminateOngoingSession) pgwRecordCOSCB->dCCATerminateOngoingSession++;
    if( cosc->serviceConditionChange.bits.cGI_SAIChange) pgwRecordCOSCB->cGI_SAIChange++;
    if( cosc->serviceConditionChange.bits.rAIChange) pgwRecordCOSCB->rAIChange++;
    if( cosc->serviceConditionChange.bits.dCCAServiceSpecificUnitExhausted) pgwRecordCOSCB->dCCAServiceSpecificUnitExhausted++;
    if( cosc->serviceConditionChange.bits.recordClosure) pgwRecordCOSCB->recordClosure++;
    if( cosc->serviceConditionChange.bits.timeLimit) pgwRecordCOSCB->timeLimit++;
    if( cosc->serviceConditionChange.bits.volumeLimit) pgwRecordCOSCB->volumeLimit++;
    if( cosc->serviceConditionChange.bits.serviceSpecificUnitLimit) pgwRecordCOSCB->serviceSpecificUnitLimit++;
    if( cosc->serviceConditionChange.bits.envelopeClosure) pgwRecordCOSCB->envelopeClosure++;
    if( cosc->serviceConditionChange.bits.eCGIChange) pgwRecordCOSCB->eCGIChange++;
    if( cosc->serviceConditionChange.bits.tAIChange) pgwRecordCOSCB->tAIChange++;
    if( cosc->serviceConditionChange.bits.userLocationChange) pgwRecordCOSCB->userLocationChange++;
    if( cosc->timeUsage)
    {
	pgwRecordCOSCB->sessions += (float)cosc->timeUsage/(float)statTimeDiv;
    }
    else
    {
      timeOfFirstUsage = mktime(&cosc->timeOfFirstUsage);
      timeOfLastUsage = mktime(&cosc->timeOfLastUsage);
      if( timeOfFirstUsage && timeOfLastUsage)
      {
	pgwRecordCOSCB->sessions += (float)(timeOfLastUsage-timeOfFirstUsage)/(float)statTimeDiv;
      }
    }
    /*Save the table name*/
    if( !losdTablesA)
      losdTablesA = calloc(1,sizeof(struct _losdTables));
    strftime(losdTablesA->name,losdTableLen,"%Y%m%d%H%M",&timeTM);
    losdTablesA->tm = timeTM;
    losdTablesB = myDB_insertRow(losdTablesDB,losdTablesA);
    if(!losdTablesB)
    {
      losdTablesA = NULL;
    }
  }
  if( pgwRecordCOCC) free( pgwRecordCOCC);
  if( pgwRecordCOSC) free( pgwRecordCOSC);
  if( cdr_ci) free(cdr_ci);
  if( cdr_pgwcdrcmb_A) free(cdr_pgwcdrcmb_A);
  if( lotvTablesA) free(lotvTablesA);
  if( losdTablesA) free(losdTablesA);
  pgwCdrStatRes.retCode = retCode;
  return retCode;
}
