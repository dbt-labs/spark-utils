This [dbt](https://github.com/fishtown-analytics/dbt) package contains macros 
that:
- can be (re)used across dbt projects running on Spark
- define Spark-specific implementations of [dispatched macros](https://docs.getdbt.com/reference/dbt-jinja-functions/adapter/#dispatch) from other packages

## Installation Instructions

Check [dbt Hub](https://hub.getdbt.com) for the latest installation 
instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) 
for more information on installing packages.

----

## Compatibility

This package provides "shims" for:
- [dbt_utils](https://github.com/fishtown-analytics/dbt-utils) (subset: cross-db utilities)
- [snowplow](https://github.com/fishtown-analytics/snowplow) (tested on Databricks only)

----

### Contributing

We welcome contributions to this repo! To contribute a new feature or a fix, 
please open a Pull Request with 1) your changes and 2) updated documentation for 
the `README.md` file.

----

### Getting started with dbt + Spark

- [What is dbt](https://docs.getdbt.com/docs/introduction)?
- [Installation](https://github.com/fishtown-analytics/dbt-spark)
- Join the #spark channel in [dbt Slack](http://slack.getdbt.com/)


## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, 
and mailing lists is expected to follow the 
[PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
