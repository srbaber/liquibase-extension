package liquibase.lockservice;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Holds the configuration for the Liquibase CDI extension.
 *
 * This is a modified version of the CDILiquibaseConfig class in the publicly available metrics-cdi library:
 * https://github.com/liquibase/liquibase/tree/master/liquibase-cdi
 *
 * Our version removes some optional configuration that we never use, and introduces some new configuration options to
 * override the changelog and lock table names. Our version also makes the configuration parameters non-optional.
 */
@Value
@Builder
@AllArgsConstructor
public class LiquibaseConfig
{
    /**
     * The liquibase contexts to activate. Normally, this should normally be "production".
     */
    @NonNull
    private String contexts;

    /**
     * The path for the changelog. This path is relative, and the way it is resolved depends on the ResourceAccessor
     * that you provide to liquibase.
     */
    @NonNull
    private String changeLogPath;

    /**
     * The name of the table that liquibase will use to track applied changes.
     *
     * Suggested format: "{shortened service name}_LQBS_CHANGELOG"
     *
     * Remember: this name has to be 32 characters or less for oracle to accept it.
     */
    @NonNull
    private String changeLogTable;

    /**
     * The name of the table that liquibase will use for locking.
     *
     * Suggested format: "{shortened service name}_LQBS_LOCK"
     *
     * Remember: this name has to be 32 characters or less for oracle to accept it.
     */
    @NonNull
    private String lockTable;
}
