#include "cdrDecoder.h"
#include "stdlib.h"
#include "tc-mydb.h"
void pgwCDRFreeStat( void *p)
{
  if( p)
    free( p);
}
unsigned int cdrPgwRecordStatClean(int coccDB, int coscDB, int pdpStatDB)
{
  unsigned int i;
  i = myDB_dropRow( coccDB, NULL, pgwCDRFreeStat);
  i += myDB_dropRow( coscDB, NULL, pgwCDRFreeStat);
  i += myDB_dropRow( pdpStatDB, NULL, pgwCDRFreeStat);
  //while( pgwRecordCOCC)
  //{
  //  pgwRecordCOCC->busy = 0;
  //  pgwRecordCOCC = pgwRecordCOCC->next;
  //}
  //while( pgwRecordCOSC)
  //{
  //  pgwRecordCOSC->busy = 0;
  //  pgwRecordCOSC = pgwRecordCOSC->next;
  //}
  return i;
}
