package liquibase.lockservice;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.spi.Extension;

import com.google.common.base.Stopwatch;
import jakarta.inject.Inject;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.resource.ResourceAccessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * A CDI wrapper for Liquibase.
 *
 * This is a modified version of the CDILiquibase class in the publicly available metrics-cdi library:
 * https://github.com/liquibase/liquibase/tree/master/liquibase-cdi
 *
 * Our version removes some optional configuration that we never use, and introduces some new configuration options to
 * override the changelog and lock table names.
 */
@Slf4j
public class LiquibaseExtension implements Extension
{
    /** System property which indicates whether to check liquibase tables during the liquibase update. */
    public static final String CHECK_LIQUIBASE_TABLES = "mp.liquibase.checkLiquibaseTables";

    /** System property which indicates whether to perform liquibase update. */
    public static final String PERFORM_UPDATE = "mp.liquibase.performUpdate";

    /** needs a common change log lock table to prevent multi-threading issues **/
    public static final String DATABASECHANGELOGLOCK = "LIQUIBASE_STARTUP_LCK";

    /** number of times to try the update and lock */
    private static final int MAX_RETRIES = 10;

    /** private locking table only I know about */
    private static final String MP_LIQUIBASE_LOCK = "MP_LIQUIBASE_LOCK";

    /**
     * Liquibase's general configuration. Needs to be @Provided by each application.
     */
    @Inject
    @LiquibaseType
    private LiquibaseConfig config;

    /**
     * Liquibase's data source. Needs to be @Provided by each application.
     */
    @Inject
    @LiquibaseType
    private DataSource dataSource;

    /**
     * Liquibase's resource accessor. Needs to be @Provided by each application.
     */
    @Inject
    @LiquibaseType
    private ResourceAccessor resourceAccessor;

    /**
     * Whether or not the database update was successful.
     */
    @Getter
    @SuppressWarnings("PMD.RedundantFieldInitializer")
    private boolean updateSuccessful = false;

    /**
     * Whether or not liquibase has attempted to update the database.
     */
    @Getter
    @SuppressWarnings("PMD.RedundantFieldInitializer")
    private boolean initialized = false;

    /** simple timer */
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    /**
     * Creates liquibase's custom database object.
     */
    private Database createDatabase(final Connection connection) throws DatabaseException
    {
        final Database database =
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

        database.setDatabaseChangeLogTableName(config.getChangeLogTable());

        database.setDatabaseChangeLogLockTableName(config.getLockTable());

        return database;
    }

    /**
     *
     */
    protected Liquibase createLiquibase(final Connection connection) throws LiquibaseException
    {
        final Liquibase liquibase =
                new Liquibase(config.getChangeLogPath(), resourceAccessor, createDatabase(connection));

        return liquibase;
    }

    /**
     * Automatically updates the database when the CDI container starts.
     */
    @PostConstruct
    public void onStartup()
    {
        try
        {
            liquibaseUpdate();
        }
        catch (final LiquibaseException e)
        {
            throw new UnexpectedLiquibaseException(e);
        }
        finally
        {
            initialized = true;
        }
    }

    /**
     * Initiate liquibase update.
     */
    private void liquibaseUpdate() throws LiquibaseException
    {
        if (System.getProperty(PERFORM_UPDATE) != null && !Boolean.getBoolean(PERFORM_UPDATE))
        {
            log.info("Liquibase Update is disabled because the system property:{} is set to false.", PERFORM_UPDATE);
            return;
        }
        performUpdate();
    }

    /**
     * Get the value of {{@value #CHECK_LIQUIBASE_TABLES} property.
     */
    private boolean checkLiquibaseTables()
    {
        final boolean checkLiquibaseTables =
                System.getProperty(CHECK_LIQUIBASE_TABLES) == null || Boolean.getBoolean(CHECK_LIQUIBASE_TABLES);
        log.info(
                "Starting Liquibase Update. System property: {} is either not set or set to {}. Liquibase update logic to check changelog tables is {}",
                CHECK_LIQUIBASE_TABLES,
                checkLiquibaseTables,
                checkLiquibaseTables ? "enabled" : "disabled");
        return checkLiquibaseTables;
    }

    /**
     * Executes the liquibase changelog.
     */
    @SuppressWarnings({
            "PMD.ConfusingTernary", "PMD.CyclomaticComplexity"
    })
    private void performUpdate() throws LiquibaseException
    {
        Connection connection = null;
        Liquibase liquibase = null;
        stopwatch.reset();
        stopwatch.start();
        try
        {
            connection = dataSource.getConnection();
            liquibase = createLiquibase(connection);

            retryingLockAndUpdate(liquibase);
            updateSuccessful = true;
        }
        catch (final SQLException e)
        {
            log.error(
                    "Exception during Liquibase Update for {} after {} seconds",
                    config.getChangeLogTable(),
                    stopwatch.elapsed(TimeUnit.SECONDS),
                    e);
            throw new DatabaseException(e);
        }
        catch (final LiquibaseException ex)
        {
            log.error(
                    "Exception during Liquibase Update for {} after {} seconds",
                    config.getChangeLogTable(),
                    stopwatch.elapsed(TimeUnit.SECONDS),
                    ex);
            throw ex;
        }
        finally
        {
            log.info(
                    "{} Liquibase Update for {} after {} seconds",
                    updateSuccessful ? "Completed" : "Failed",
                    config.getChangeLogTable(),
                    stopwatch.elapsed(TimeUnit.SECONDS));
            /*
             * If we managed to create a liquibase Database, we'll close which will also close the connection. If we
             * didn't get that far, we'll try to close connection explicitly.
             */
            if (liquibase != null && liquibase.getDatabase() != null)
            {
                liquibase.getDatabase().close();
            }
            else if (connection != null)
            {
                try
                {
                    connection.rollback();
                    connection.close();
                }
                catch (final SQLException e)
                {
                    LoggerFactory.getLogger(getClass()).warn("Failed to close connection", e);
                    // nothing to do
                }
            }
        }
    }

    /**
     * allow for retrying on the lock and update process
     */
    private void retryingLockAndUpdate(final Liquibase liquibase) throws LiquibaseException
    {
        final FirstInFirstOutLockService lockService = new FirstInFirstOutLockService();
        lockService.setLockTableName(MP_LIQUIBASE_LOCK);
        lockService.setDatabase(liquibase.getDatabase());

        int numRetries = 0;
        while (true)
        {
            try
            {
                log.debug(
                        "Requesting Liquibase Update for {} after {}",
                        config.getChangeLogTable(),
                        stopwatch.elapsed(TimeUnit.SECONDS));

                lockService.waitForLock();
                liquibase.update(new Contexts(config.getContexts()), new LabelExpression(), checkLiquibaseTables());
                lockService.releaseLock();
                return;
            }
            catch (final LiquibaseException ex)
            {
                log.error(
                        "Failure during Liquibase Update/Lock for {} because {}",
                        config.getChangeLogTable(),
                        ex.toString());

                if (numRetries > MAX_RETRIES - 2)
                {
                    log.error(
                            "Approaching timeout during Liquibase Update for {} after {} seconds",
                            config.getChangeLogTable(),
                            stopwatch.elapsed(TimeUnit.SECONDS));
                }
                if (numRetries > MAX_RETRIES)
                {
                    log.error(
                            "Exceeded timeout during Liquibase Update for {} after {} seconds",
                            config.getChangeLogTable(),
                            stopwatch.elapsed(TimeUnit.SECONDS),
                            ex);
                    throw ex;
                }
            }
            numRetries++;
        }
    }
}
