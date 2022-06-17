package liquibase.lockservice;

import liquibase.database.Database;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class FirstInFirstOutLockServiceTest {
    private final static String hostIp = "192.168.01 (192.168.0.1)";
    private final static String id = UUID.randomUUID().toString();
    private final static Database database = null;

    @Test
    public void setterGetterBuilderTest()
    {
        LockRecord lockRecord = new LockRecord();
        lockRecord.setId(id);
        lockRecord.setLocked(true);
        lockRecord.setLockGranted(100L);
        lockRecord.setLockRequested(200L);
        lockRecord.setLockedBy(hostIp);
        lockRecord.setPriority(1);

        LockRecord buildRecord = LockRecord.builder()
                .id(id)
                .locked(true)
                .lockGranted(100L)
                .lockRequested(200L)
                .lockedBy(hostIp)
                .priority(1)
                .build();

        assertEquals("ID round trip", lockRecord.getId(), buildRecord.getId());
        assertEquals("LOCKED round trip", lockRecord.isLocked(), buildRecord.isLocked());
        assertEquals("LOCKGRANTED round trip", lockRecord.getLockGranted(), buildRecord.getLockGranted());
        assertEquals("LOCKREQUESTED round trip", lockRecord.getLockRequested(), buildRecord.getLockRequested());
        assertEquals("LOCKEDBY round trip", lockRecord.getLockedBy(), buildRecord.getLockedBy());
        assertEquals("PRIORITY round trip", lockRecord.getPriority(), buildRecord.getPriority());
    }

    public void singleThreadLock()
    {
        FirstInFirstOutLockService lockService = new FirstInFirstOutLockService();
        lockService.setLockPriority(1);
        lockService.setChangeLogLockRecheckTime(10);
        lockService.setChangeLogLockWaitTime(1);
        lockService.setLockTableName("BOBS_YOUR_UNCLE");
        lockService.setDatabase(database);
    }
}
