from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(dag, start):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="materialize_views") as group:
        update_materialize_views_start = DummyOperator(
            task_id="update_materialize_views_start"
        )
        start >> update_materialize_views_start

        update_materialize_views_end = DummyOperator(
            task_id="update_materialize_views_end"
        )

        # Geometry tables
        query_CountryRaw_table = OlvinBigQueryOperator(
            task_id="query_CountryRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/countryraw.sql" %}',
        )
        update_materialize_views_start >> query_CountryRaw_table >> update_materialize_views_end

        query_StateRaw_table = OlvinBigQueryOperator(
            task_id="query_StateRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/stateraw.sql" %}',
        )
        update_materialize_views_start >> query_StateRaw_table >> update_materialize_views_end

        query_CensusTract_table = OlvinBigQueryOperator(
            task_id="query_CensusTract_table",
            query='{% include "./bigquery/materialized_views_eqv/censustract.sql" %}',
        )
        update_materialize_views_start >> query_CensusTract_table >> update_materialize_views_end

        query_CityRaw_table = OlvinBigQueryOperator(
            task_id="query_CityRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/cityraw.sql" %}',
        )
        update_materialize_views_start >> query_CityRaw_table >> update_materialize_views_end

        query_ZipCodeRaw_table = OlvinBigQueryOperator(
            task_id="query_ZipCodeRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/zipcoderaw.sql" %}',
        )
        update_materialize_views_start >> query_ZipCodeRaw_table >> update_materialize_views_end

        # Pois / Centers / Brands
        query_SGPlaceRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplaceraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceRaw_table >> update_materialize_views_end

        query_SGPlaceActivity_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceActivity_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplaceactivity.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceActivity_table >> update_materialize_views_end

        query_SGCenterRaw_table = OlvinBigQueryOperator(
            task_id="query_SGCenterRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgcenterraw.sql" %}',
        )
        update_materialize_views_start >> query_SGCenterRaw_table >> update_materialize_views_end

        query_SGBrandRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandRaw_table >> update_materialize_views_end

        query_SGPlaceSearch_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceSearch_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacesearch.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceSearch_table >> update_materialize_views_end

        query_SGBrandSearch_table = OlvinBigQueryOperator(
            task_id="query_SGBrandSearch_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandsearch.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandSearch_table >> update_materialize_views_end

        # POI visits
        query_SGPlaceHourlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceHourlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacehourlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceHourlyVisitsRaw_table >> update_materialize_views_end

        query_SGPlaceDailyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceDailyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacedailyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceDailyVisitsRaw_table >> update_materialize_views_end

        query_SGPlaceMonthlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceMonthlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacemonthlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceMonthlyVisitsRaw_table >> update_materialize_views_end

        # Brand Agg visits
        query_SGBrandHourlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandHourlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandhourlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandHourlyVisitsRaw_table >> update_materialize_views_end

        query_SGBrandDailyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandDailyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbranddailyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandDailyVisitsRaw_table >> update_materialize_views_end

        query_SGBrandMonthlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandMonthlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandmonthlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandMonthlyVisitsRaw_table >> update_materialize_views_end

        # Brand-State Agg visits
        query_SGBrandStateHourlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandStateHourlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandstatehourlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandStateHourlyVisitsRaw_table >> update_materialize_views_end

        query_SGBrandStateDailyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandStateDailyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandstatedailyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandStateDailyVisitsRaw_table >> update_materialize_views_end

        query_SGBrandStateMonthlyVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGBrandStateMonthlyVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgbrandstatemonthlyvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGBrandStateMonthlyVisitsRaw_table >> update_materialize_views_end

        # City/Zipcode aggs
        query_CityZipCodesMonthlyVisits_table = OlvinBigQueryOperator(
            task_id="query_CityZipCodesMonthlyVisits_table",
            query='{% include "./bigquery/materialized_views_eqv/cityzipcodesmonthlyvisits.sql" %}',
        )
        update_materialize_views_start >> query_CityZipCodesMonthlyVisits_table >> update_materialize_views_end

        query_CityPatternsActivityRaw_table = OlvinBigQueryOperator(
            task_id="query_CityPatternsActivityRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/citypatternsactivityraw.sql" %}',
        )
        update_materialize_views_start >> query_CityPatternsActivityRaw_table >> update_materialize_views_end

        query_ZipCodePatternsActivityRaw_table = OlvinBigQueryOperator(
            task_id="query_ZipCodePatternsActivityRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/zipcodepatternsactivityraw.sql" %}',
        )
        update_materialize_views_start >> query_ZipCodePatternsActivityRaw_table >> update_materialize_views_end

        # Other POI tables
        query_SGPlaceCameoMonthlyRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceCameoMonthlyRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacecameomonthlyraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceCameoMonthlyRaw_table >> update_materialize_views_end

        query_TenantFinderCameoScores_table = OlvinBigQueryOperator(
            task_id="query_TenantFinderCameoScores_table",
            query='{% include "./bigquery/materialized_views_eqv/tenantfindercameoscores.sql" %}',
        )
        update_materialize_views_start >> query_TenantFinderCameoScores_table >> update_materialize_views_end

        query_SGPlaceHomeZipCodeYearly_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceHomeZipCodeYearly_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacehomezipcodeyearly.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceHomeZipCodeYearly_table >> update_materialize_views_end

        query_SGPlacePatternVisitsRaw_table = OlvinBigQueryOperator(
            task_id="query_SGPlacePatternVisitsRaw_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacepatternvisitsraw.sql" %}',
        )
        update_materialize_views_start >> query_SGPlacePatternVisitsRaw_table >> update_materialize_views_end

        query_SGPlaceVisitorBrandDestinations_table = OlvinBigQueryOperator(
            task_id="query_SGPlaceVisitorBrandDestinations_table",
            query='{% include "./bigquery/materialized_views_eqv/sgplacevisitorbranddestinations.sql" %}',
        )
        update_materialize_views_start >> query_SGPlaceVisitorBrandDestinations_table >> update_materialize_views_end

        # Other tables
        query_BrandMallLocations_table = OlvinBigQueryOperator(
            task_id="query_BrandMallLocations_table",
            query='{% include "./bigquery/materialized_views_eqv/brandmalllocations.sql" %}',
        )
        update_materialize_views_start >> query_BrandMallLocations_table >> update_materialize_views_end

        # DateRange table (needs some postgres_batch tables - see sql or render)
        query_DateRange_table = OlvinBigQueryOperator(
            task_id="query_DateRange_table",
            query='{% include "./bigquery/materialized_views_eqv/daterange.sql" %}',
        )
        update_materialize_views_end >> query_DateRange_table

    return group