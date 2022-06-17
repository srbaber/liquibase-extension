package liquibase.lockservice;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * simple class for holding the lock table data
 */
@Data
@Slf4j
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LockRecord
{
    /**
     * id
     */
    private String id;

    /**
     * priority
     */
    private int priority;

    /**
     * when requested
     */
    private long lockRequested;

    /**
     * is locked
     */
    private boolean locked;

    /**
     * when granted
     */
    private long lockGranted;

    /**
     * host that requested
     */
    private String lockedBy;
}
