package liquibase.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.core.InsertOrUpdateStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.NetUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * a reentrant, priority fifo queue locking service for liquibase update.
 */
@Slf4j
@Getter
@SuppressWarnings({
        "PMD", "checkstyle:methodcount"
})
public class FirstInFirstOutLockService implements LockService
{
    /**
     * how many seconds should the spin lock wait
     */
    private static final long DEFAULT_SECONDS_TO_SPIN = 5;

    /**
     * how many minutes should the spin lock try for a lock
     */
    private static final long DEFAULT_MINUTES_TO_LOCK = 10;

    /**
     * a single lock table to rule them all
     */
    @Setter
    private String lockTableName;

    /**
     * the uuid of this multi-server / multi-thread locker
     */
    private final String uuid = UUID.randomUUID().toString();

    /**
     * higher priority
     */
    private final int priority = 0;

    /**
     * the database handle for primitive operations
     */
    @Setter
    private Database database;

    /**
     * how long before a lock request is expired
     */
    @Setter
    private long changeLogLockWaitTime = DEFAULT_MINUTES_TO_LOCK; // minutes

    /**
     * how often to poll the lock table
     */
    @Setter
    private long changeLogLockRecheckTime = DEFAULT_SECONDS_TO_SPIN; // seconds

    /**
     * allow different lock services to set up different priorities
     */
    @Setter
    private int lockPriority = 1;

    @Override
    public boolean supports(final Database dbase)
    {
        return true;
    }

    @Override
    public boolean hasChangeLogLock()
    {
        final LockRecord lockRecord = getLockRecord();
        return lockRecord != null && lockRecord.isLocked();
    }

    /**
     * spin wait for the lock
     */
    @Override
    public void waitForLock() throws LockException
    {
        log.debug("Waiting for lock for Liquibase Update for {}", this.database.getDatabaseChangeLogTableName());

        init();
        final LockRecord existingRecord = getLockRecord();
        if (existingRecord == null)
        {
            createLockRecord();
        }

        while (true)
        {
            final boolean locked = acquireLock();
            if (locked)
            {
                break;
            }
            else if (expired())
            {
                removeLockRecord();
                throw new LockException("Timeout waiting for lock for Liquibase Update");
            }
            else
            {
                spin();
            }
        }
        log.debug("Acquired lock for Liquibase Update for {}", this.database.getDatabaseChangeLogTableName());
    }

    /**
     * test if the lock record has expired
     */
    private boolean expired()
    {
        final LockRecord lockRecord = getLockRecord();
        return lockRecord == null
                || lockRecord.getLockRequested() < new Date().getTime() - getChangeLogLockWaitTimeInMillis();
    }

    /**
     * converts the lock wait time from minutes to useful millis
     */
    private long getChangeLogLockWaitTimeInMillis()
    {
        return TimeUnit.MINUTES.toMillis(changeLogLockWaitTime);
    }

    /**
     * sleep for the spin lock
     */
    private void spin()
    {
        try
        {
            Thread.sleep(getChangeLogLockRecheckTimeInMillis());
        }
        catch (final InterruptedException exc)
        {
            log.error("Interrupted while sleeping for Liquibase Update lock", exc);
        }
    }

    /**
     * converts the lock retry time from seconds into useful millis
     */
    private long getChangeLogLockRecheckTimeInMillis()
    {
        return TimeUnit.SECONDS.toMillis(changeLogLockRecheckTime);
    }

    /**
     * returns the current scope jdbc executor
     */
    private Executor getExecutor()
    {
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this.database);
    }

    /**
     * get the lock record for this lock service
     */
    private LockRecord getLockRecord()
    {
        try
        {
            return fromResultSet(getExecutor().queryForList(selectById()));
        }
        catch (final DatabaseException e)
        {
            log.error("No lock record for Liquibase Update uuid {}", uuid, e);
            return null;
        }
    }

    /**
     * get the next lock record in the queue
     */
    private LockRecord nextWaitingLock()
    {
        try
        {
            LockRecord lockEntity = fromResultSet(getExecutor().queryForList(selectLocked()));
            if (lockEntity == null)
            {
                lockEntity = fromResultSet(getExecutor().queryForList(selectNext()));
            }
            return lockEntity;
        }
        catch (final DatabaseException e)
        {
            log.error("No next lock record for Liquibase Update", e);
            return null;
        }
    }

    /**
     * converts the result set into a single result object
     */
    private LockRecord fromResultSet(final List<Map<String, ?>> resultSet)
    {
        if (resultSet == null || resultSet.isEmpty())
        {
            return null;
        }
        final Map<String, ?> result = resultSet.get(0);
        return fromResult(result);
    }

    /**
     * converts the result into a single result object
     */
    private LockRecord fromResult(final Map<String, ?> result)
    {
        return LockRecord.builder()
            .id((String) result.get("ID"))
            .lockRequested(longValue(result.get("LOCKREQUESTED")))
            .lockGranted(longValue(result.get("LOCKGRANTED")))
            .lockedBy((String) result.get("LOCKEDBY"))
            .locked(intValue(result.get("LOCKED")) != 0)
            .priority(intValue(result.get("PRIORITY")))
            .build();
    }

    /**
     * get a value as long from the result set
     */
    private long longValue(final Object value)
    {
        long result = 0;
        if (value instanceof BigDecimal)
        {
            result = ( (BigDecimal) value ).longValue();
        }
        if (value instanceof Integer)
        {
            result = ( (Integer) value ).longValue();
        }
        return result;
    }

    /**
     * get a value as long from the result set
     */
    private int intValue(final Object value)
    {
        int result = 0;
        if (value instanceof BigDecimal)
        {
            result = ( (BigDecimal) value ).intValue();
        }
        if (value instanceof Integer)
        {
            result = ( (Integer) value ).intValue();
        }
        return result;
    }

    /**
     * all the fields are included in the base select
     */
    private String baseSelect()
    {
        return "SELECT ID, PRIORITY, LOCKREQUESTED, LOCKED, LOCKGRANTED, LOCKEDBY FROM " + getLockTableName();
    }

    /**
     * query the database for the record with the matching id
     */
    private SqlStatement selectById()
    {
        return new RawSqlStatement(baseSelect() + " WHERE ID = '" + uuid + "'");
    }

    /**
     * query the database for the locked record
     */
    private SqlStatement selectLocked()
    {
        return new RawSqlStatement(baseSelect() + " WHERE LOCKED = 1");
    }

    /**
     * query the database for the next record to be locked based on the priority and fifo
     */
    private SqlStatement selectNext()
    {
        return new RawSqlStatement(baseSelect() + " ORDER BY PRIORITY, LOCKREQUESTED, ID");
    }

    /**
     * return the table name that is backing the locks
     */
    private String getLockTableName()
    {
        if (lockTableName != null)
        {
            return lockTableName;
        }
        return database.getDatabaseChangeLogLockTableName();
    }

    /**
     * try and update the database record to indicate we have the lock. should be 'automic'
     */
    private int performLock()
    {
        try
        {
            final UpdateStatement updateStatement = new UpdateStatement(
                    database.getLiquibaseCatalogName(),
                    database.getLiquibaseSchemaName(),
                    getLockTableName());
            updateStatement.addNewColumnValue("LOCKED", 1);
            updateStatement.addNewColumnValue("LOCKGRANTED", new Date().getTime());
            updateStatement.setWhereClause(
                    "ID = '" + uuid + "' AND LOCKED = 0 AND (select count(*) from " + getLockTableName()
                            + " WHERE LOCKED = 1) = 0");
            database.rollback();
            final int count = getExecutor().update(updateStatement);
            database.commit();
            return count;
        }
        catch (final DatabaseException e)
        {
            log.error("Unable to delete lock record for Liquibase Update for uuid {}", uuid, e);
        }
        return 0;
    }

    /**
     * remove the lock request record
     */
    private void removeLockRecord()
    {
        try
        {
            final DeleteStatement deleteStatement = new DeleteStatement(
                    database.getLiquibaseCatalogName(),
                    database.getLiquibaseSchemaName(),
                    getLockTableName());
            deleteStatement.setWhere("ID = '" + uuid + "'");
            database.rollback();
            getExecutor().execute(deleteStatement);
            database.commit();
        }
        catch (final DatabaseException e)
        {
            log.error("Unable to delete lock record for Liquibase Update for uuid {}", uuid, e);
        }
    }

    /**
     * remove the expired lock request records
     */
    private void removeExpiredLocks()
    {
        try
        {
            final DeleteStatement deleteStatement = new DeleteStatement(
                    database.getLiquibaseCatalogName(),
                    database.getLiquibaseSchemaName(),
                    getLockTableName());
            deleteStatement
                .setWhere("LOCKREQUESTED < " + ( new Date().getTime() - getChangeLogLockWaitTimeInMillis() ));
            database.rollback();
            getExecutor().execute(deleteStatement);
            database.commit();
        }
        catch (final DatabaseException e)
        {
            log.error("Unable to delete expired lock records for Liquibase Update for uuid {}", uuid, e);
        }
    }

    /**
     * remove all the lock request record
     */
    private void removeAllLocks()
    {
        try
        {
            if (database != null)
            {
                final DeleteStatement deleteStatement = new DeleteStatement(
                        database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(),
                        getLockTableName());
                database.rollback();
                getExecutor().execute(deleteStatement);
                database.commit();
            }
        }
        catch (final DatabaseException e)
        {
            log.error("Unable to delete lock records for Liquibase Update", e);
        }
    }

    /**
     * create a lock record
     */
    private void createLockRecord()
    {
        try
        {
            final InsertOrUpdateStatement insertStatement = new InsertOrUpdateStatement(
                    database.getLiquibaseCatalogName(),
                    database.getLiquibaseSchemaName(),
                    getLockTableName(),
                    "ID");
            insertStatement.addColumnValue("ID", uuid);
            insertStatement
                .addColumnValue("LOCKEDBY", NetUtil.getLocalHostName() + "(" + NetUtil.getLocalHostAddress() + ")");
            insertStatement.addColumnValue("LOCKGRANTED", 0);
            insertStatement.addColumnValue("LOCKED", 0);
            insertStatement.addColumnValue("PRIORITY", lockPriority);
            insertStatement.addColumnValue("LOCKREQUESTED", new Date().getTime());
            database.rollback();
            getExecutor().execute(insertStatement);
            database.commit();
        }
        catch (final UnknownHostException | SocketException | DatabaseException exc)
        {
            log.error("Unable to create lock record for Liquibase Update", exc);
        }
    }

    /**
     * try and get the lock table entry
     */
    @Override
    public boolean acquireLock() throws LockException
    {
        log.debug("Locking for Liquibase Update for {}", this.database.getDatabaseChangeLogTableName());

        boolean locked = false;

        removeExpiredLocks();
        final LockRecord nextRecord = nextWaitingLock();
        if (nextRecord == null)
        {
            throw new LockException("Expired waiting for lock for Liquibase Update");
        }
        log.debug("Next lock/Locked record for Liquibase Update of {} is for {}", uuid, nextRecord.getId());
        if (nextRecord.getId().equals(uuid))
        {
            if (nextRecord.isLocked())
            {
                log.debug("Previously locked for Liquibase Update of {}", uuid);
                locked = true;
            }
            else
            {
                final int count = performLock();
                if (count == 1)
                {
                    log.debug("Performed lock for Liquibase Update of {}", uuid);
                    locked = true;
                }
                else
                {
                    log.debug("Aborted lock for Liquibase Update of {}", uuid);
                }
            }
        }

        log.debug(
                "{} for Liquibase Update of {} for {}",
                locked ? "Locked" : "Not locked",
                uuid,
                this.database.getDatabaseChangeLogTableName());

        return locked;
    }

    /**
     * release the lock entry
     */
    public void releaseLock() throws LockException
    {
        log.debug("Unlocking for Liquibase Update for {}", this.database.getDatabaseChangeLogTableName());

        removeLockRecord();

        log.debug("Unlocked for Liquibase Update for {}", this.database.getDatabaseChangeLogTableName());
    }

    @Override
    public DatabaseChangeLogLock[] listLocks() throws LockException
    {
        final DatabaseChangeLogLock[] emptyList = new DatabaseChangeLogLock[0];
        try
        {
            final List<Map<String, ?>> resultSet = getExecutor().queryForList(selectNext());
            final AtomicInteger index = new AtomicInteger(0);
            return resultSet.stream()
                .map(this::fromResult)
                .map(
                        lockRecord -> new DatabaseChangeLogLock(
                                index.getAndIncrement(),
                                new Date(lockRecord.getLockRequested()),
                                lockRecord.getId()))
                .collect(Collectors.toList())
                .toArray(emptyList);
        }
        catch (final DatabaseException e)
        {
            return emptyList;
        }
    }

    @Override
    public void forceReleaseLock() throws LockException, DatabaseException
    {
        releaseLock();
    }

    @Override
    public void reset()
    {
        removeAllLocks();
    }

    @Override
    public void init()
    {
        if (!changeLogLockTableExists())
        {
            createLockTable();
        }
    }

    @Override
    public void destroy() throws DatabaseException
    {
    }

    /**
     * determine if the table exists in the database
     */
    private boolean changeLogLockTableExists()
    {
        try
        {
            return SnapshotGeneratorFactory.getInstance()
                .has(
                        ( new Table() ).setName(getLockTableName())
                            .setSchema(
                                    new Schema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName())),
                        database);
        }
        catch (final LiquibaseException var3)
        {
            return false;
        }
    }

    /**
     * creates the lock table
     */
    private void createLockTable()
    {
        try
        {
            final String charTypeName = this.getCharTypeName();
            final CreateTableStatement createTableStatement = new CreateTableStatement(
                    database.getLiquibaseCatalogName(),
                    database.getLiquibaseSchemaName(),
                    getLockTableName())
                        .setTablespace(database.getLiquibaseTablespaceName())
                        .addPrimaryKeyColumn(
                                "ID",
                                DataTypeFactory.getInstance().fromDescription(charTypeName + "(255)", database),
                                null,
                                null,
                                null,
                                new NotNullConstraint())
                        .addColumn("PRIORITY", DataTypeFactory.getInstance().fromDescription("int", database))
                        .addColumn(
                                "LOCKREQUESTED",
                                DataTypeFactory.getInstance().fromDescription("numeric" + "(19)", database))
                        .addColumn(
                                "LOCKED",
                                DataTypeFactory.getInstance().fromDescription("int", database),
                                null,
                                null,
                                new NotNullConstraint())
                        .addColumn(
                                "LOCKGRANTED",
                                DataTypeFactory.getInstance().fromDescription("numeric" + "(19)", database))
                        .addColumn(
                                "LOCKEDBY",
                                DataTypeFactory.getInstance().fromDescription(charTypeName + "(255)", database));

            final SqlStatement[] statements = {
                    createTableStatement
            };

            final List<SqlVisitor> visitors = new ArrayList();
            database.rollback();
            database.execute(statements, visitors);
            database.commit();
        }
        catch (final LiquibaseException exc)
        {
            log.error("Unable to create lock table for Liquibase Update", exc);
        }
    }

    /**
     * special handling of varchar type name on mssql
     */
    protected String getCharTypeName()
    {
        return database instanceof MSSQLDatabase && ( (MSSQLDatabase) database ).sendsStringParametersAsUnicode()
                ? "nvarchar"
                : "varchar";
    }
}
