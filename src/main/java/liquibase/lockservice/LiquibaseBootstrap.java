package liquibase.lockservice;

import jakarta.enterprise.inject.spi.InjectionTargetFactory;
import lombok.AllArgsConstructor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.AnnotatedType;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.enterprise.inject.spi.InjectionTarget;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Observes CDI container startup events and triggers the Liquibase update process.
 *
 * This is a modified version of the CDIBootstrap class in the publicly available metrics-cdi library:
 * https://github.com/liquibase/liquibase/tree/master/liquibase-cdi
 */
public class LiquibaseBootstrap implements Extension
{
    /**
     * CDI Bean that manages the CDILiquibase object.
     */
    @AllArgsConstructor
    private static class CDILiquibaseBean implements Bean<LiquibaseExtension>
    {
        /**
         * The injection target.
         */
        private final InjectionTarget<LiquibaseExtension> injectionTarget;

        @Override
        public LiquibaseExtension create(final CreationalContext<LiquibaseExtension> ctx)
        {
            final LiquibaseExtension cdiLiquibase = injectionTarget.produce(ctx);
            injectionTarget.inject(cdiLiquibase, ctx);
            injectionTarget.postConstruct(cdiLiquibase);
            return cdiLiquibase;
        }

        @Override
        public void destroy(final LiquibaseExtension cdiLiquibase, final CreationalContext<LiquibaseExtension> ctx)
        {
            injectionTarget.preDestroy(cdiLiquibase);
            injectionTarget.dispose(cdiLiquibase);
            ctx.release();
        }

        @Override
        public Class<?> getBeanClass()
        {
            return LiquibaseExtension.class;
        }

        @Override
        public Set<InjectionPoint> getInjectionPoints()
        {
            return injectionTarget.getInjectionPoints();
        }

        @Override
        public String getName()
        {
            return "cdiLiquibase";
        }

        @Override
        public Set<Annotation> getQualifiers()
        {
            final Set<Annotation> qualifiers = new HashSet<Annotation>();
            qualifiers.add(new Default.Literal());
            qualifiers.add(new Any.Literal());
            return qualifiers;
        }

        @Override
        public Class<? extends Annotation> getScope()
        {
            return ApplicationScoped.class;
        }

        @Override
        public Set<Class<? extends Annotation>> getStereotypes()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<Type> getTypes()
        {
            final Set<Type> types = new HashSet<Type>();
            types.add(LiquibaseExtension.class);
            types.add(Object.class);
            return types;
        }

        @Override
        public boolean isAlternative()
        {
            return false;
        }
    }

    /**
     * The CDILiquibase instance created by this extension.
     */
    private Bean<LiquibaseExtension> bean;

    /**
     * Creates CDILiquibase bean and makes the context aware of it.
     */
    void afterBeanDiscovery(@Observes final AfterBeanDiscovery afterBeanDiscovery, final BeanManager beanManager)
    {
        final AnnotatedType<LiquibaseExtension> annotatedType =
                beanManager.createAnnotatedType(LiquibaseExtension.class);
        final InjectionTargetFactory<LiquibaseExtension> factory = beanManager.getInjectionTargetFactory(annotatedType);
        final InjectionTarget<LiquibaseExtension> injectionTarget = factory.createInjectionTarget(bean);
        bean = new CDILiquibaseBean(injectionTarget);
        afterBeanDiscovery.addBean(bean);
    }

    /**
     * Runs after deployment validation - appears to force the CDILiquibase bean to be created in the CDI context,
     * regardless of whether something else references it.
     */
    void afterDeploymentValidation(@Observes final AfterDeploymentValidation event, final BeanManager manager)
    {
        try
        {
            /*
             * The call to toString() is critically important here (for unknown reasons). Removing it makes liquibase
             * updates get skipped.
             */
            manager.getReference(bean, bean.getBeanClass(), manager.createCreationalContext(bean)).toString();
        }
        catch (final Exception ex)
        {
            event.addDeploymentProblem(ex);
        }
    }
}
