package liquibase.lockservice;

import jakarta.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Qualifier annotation to be placed on service-specific @Provider methods that create objects used for configuring
 * Liquibase, such as the database connection, and the CDILiquibaseConfig.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
        FIELD, METHOD, PARAMETER, TYPE
})
@Qualifier
public @interface LiquibaseType
{
}
