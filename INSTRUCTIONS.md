# Dagster & DBT

In this demo, we’ll focus on integrating a dbt project with Dagster from end to end. We’ll use data from [NYC OpenData](https://opendata.cityofnewyork.us).

By the end of the course, you will:

* Create dbt models and load them into Dagster as assets
* Run dbt and store the transformed data in a DuckDB database
* Apply partitions to incremental dbt models

<details>
  <summary><h2>Lesson 0: Introduction</h2></summary>

In this course, you'll learn how to integrate and orchestrate dbt projects with Dagster. You'll load dbt models into Dagster as assets, build dependencies, and ready your project for production deployment.

In the world of ETL/ELT, dbt - that’s right, all lowercase - is the ‘T’ in the process of Extracting, Loading, and **Transforming** data. Using familiar languages like SQL and Python, dbt is open-source software that allows users to write and run data transformations against the data loaded into their data warehouses.

dbt isn’t popular only for its easy, straightforward adoption, but also because it embraces software engineering best practices. Data analysts can use skills they already have - like SQL expertise - and simultaneously take advantage of:

* **Keeping things DRY (Don’t Repeat Yourself).** dbt models, which are business definitions represented in SQL `SELECT` statements, can be referenced in other models. Focusing on modularity allows you to reduce bugs, standardize analytics logic, and get a running start on new analyses.
* **Automatically managing dependencies and generating documentation.** Dependencies between models are not only easy to declare, they’re automatically managed by dbt. Additionally, dbt also generates a DAG (directed acyclic graph), which shows how models in a dbt project relate to each other.
* **Preventing negative impact on end-users.** Support for multiple environments ensures that development can occur without impacting users in production.

Dagster’s approach to building data platforms maps directly to these same best practices, making dbt and Dagster a natural, powerful pairing.

At a glance, it might seem like Dagster and dbt do the same thing. Both technologies, after all, work with data assets and are instrumental in modern data platforms.

However, dbt Core can only transform data that is already in a data warehouse - it can’t extract from a source, load it into its final destination, or automate either of these operations. And while you could use dbt Cloud’s native features to schedule running your models, other portions of your data pipelines - such as Fivetran-ingested tables or data from Amazon S3 - won’t be included.

To have everything running together, you need an orchestrator. This is where Dagster comes in:

> Dagster’s core design principles go really well together with dbt. The similarities between the way that Dagster thinks about data pipelines and the way that dbt thinks about data pipelines means that Dagster can orchestrate dbt much more faithfully than other general-purpose orchestrators like Airflow.
>
> At the same time, Dagster is able to compensate for dbt’s biggest limitations. dbt is rarely used in a vacuum: the data transformed using dbt needs to come from somewhere and go somewhere. When a data platform needs more than just dbt, Dagster is a better fit than dbt-specific orchestrators, like the job scheduling system inside dbt Cloud. ([source](https://dagster.io/blog/orchestrating-dbt-with-dagster))

At a glance, using dbt alongside Dagster gives analytics and data engineers the best of both their worlds:

* **Analytics engineers** can author analytics code in a familiar language while adhering to software engineering best practices
* **Data engineers** can easily incorporate dbt into their organization’s wider data platform, ensuring observability and reliability

There’s more, however. Other orchestrators will provide you with one of two less-than-appealing options: running dbt as a single task that lacks visibility, or running each dbt model as an individual task and pushing the execution into the orchestrator, which goes against how dbt is intended to be run.

Using dbt with Dagster is unique, as Dagster separates data assets from the execution that produces them and gives you the ability to monitor and debug each dbt model individually.
</details>

<details>
  <summary><h2>Lesson 1: Setup dbt project</h2></summary>

  We have a dbt project in the `analytics` directory. Throughout the duration of this module, you’ll add new dbt models and see them reflected in Dagster.

  To begin let's install the dbt package dependencies:

  <button data-command="run:cd analytics && dbt deps">Run `dbt deps`</button>

  Now, take a look at the dbt models we are going to use to get started:

  <button data-command="open:analytics/models/sources/raw_taxis.yml">Open `analytics/models/sources/raw_taxis.yml`</button>
  <button data-command="open:analytics/models/staging/staging.yml">Open `analytics/models/staging/staging.yml`</button>
  <button data-command="open:analytics/models/staging/stg_trips.sql">Open `analytics/models/staging/stg_trips.sql`</button>
  <button data-command="open:analytics/models/staging/stg_zones.sql">Open `analytics/models/staging/stg_zones.sql`</button>

  Two other important dbt files of note:

  <button data-command="open:analytics/dbt_project.yml">Open `analytics/dbt_project.yml`</button>
  <button data-command="open:analytics/profiles.yml">Open `analytics/profiles.yml`</button>

  Finally, lets run the dbt project

  <button data-command="run:cd analytics &&  dbt build">Run `dbt build`</button>

</details>

<details>
  <summary><h2>Lesson 2: Connecting dbt & Dagster</h2></summary>

  ### Constructing the dbt project

  Independent of Dagster, running most dbt commands creates a set of files in a new directory called `target`. The most important file is the `manifest.json`. More commonly referred to as “the manifest file,” this file is a complete representation of your dbt project in a predictable format.

When Dagster builds your code location, it reads the manifest file to discover the dbt models and turn them into Dagster assets. There are a variety of ways to build the `manifest.json` file. However, we recommend using the `dbt parse` CLI command.

  <button data-command="run:cd analytics &&  dbt parse">Run `dbt parse`</button>

  ### Representing the dbt project in Dagster

  As you’ll frequently point your Dagster code to the `target/manifest.json` file and your dbt project in this course, it’ll be helpful to keep a reusable representation of the dbt project.
  This can be easily done using the `DbtProject` class.

  In the `dagster_university` directory, in the `project.py` file, let's add the following code:

<insert-text file="./dagster_university/project.py" line="4" col="0">
```python
dbt_project = DbtProject(
  project_dir=Path(__file__).joinpath("..", "..", "analytics").resolve(),
)  # This code creates a representation of the dbt project called dbt_project.
```
</insert-text>

  ### Creating a Dagster resource to run dbt

  Our next step is to define a Dagster resource as the entry point used to run dbt commands and configure its execution.

  The `DbtCliResource` is the main resource that you’ll be working with. In later sections, we’ll walk through some of the resource’s methods and how to customize what Dagster does when dbt runs.

  > *💡 **Resource refresher**: Resources are Dagster’s recommended way of connecting to other services and tools, such as dbt, your data warehouse, or a BI tool.*

  We create the resource in `dagster_university/resources/__init__.py`, which is where other resources are defined.

  <insert-text file="./dagster_university/project.py" line="4" col="0">
  ```
  from ..project import dbt_project        # Imports the dbt_project representation we just defined

  dbt_resource = DbtCliResource(           # Instantiate a new DbtCliResource
      project_dir=dbt_project,             # Tell the resource that the dbt project to execute is the dbt_project
  )
  ```
  </insert-text>

  ### Loading dbt models into Dagster as assets

  #### Turning dbt models into assets with @dbt_assets

  The star of the show here is the `@dbt_assets` decorator. This is a specialized asset decorator that wraps around a dbt project to tell Dagster what dbt models exist. In the body of the `@dbt_assets` definition, you write exactly how you want Dagster to run your dbt models.

  Many Dagster projects may only need one `@dbt_assets`-decorated function that manages the entire dbt project. However, you may need to create multiple definitions for various reasons, such as:

  * You have multiple dbt projects
  * You want to exclude certain dbt models
  * You want to only execute dbt run and not dbt build on specific models
  * You want to customize what happens after certain models finish, such as sending a notification
  * You need to configure some sets of models differently

  We’ll only create one `@dbt_assets` definition for now, but in a later lesson, we’ll encounter a use case for needing another `@dbt_assets` definition.

  #### Loading the models as assets
  Let's insert the following code into `assets/dbt.py`
  ```
  from ..project import dbt_project

  # The @dbt_assets decorator creates a new asset function and provides it with a reference to the project's manifest file
  @dbt_assets(
      manifest=dbt_project.manifest_path,
  )
  def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
  ```
  Notice we provided two arguments to the `dbt_analytics` function. The first argument is the `context`, which indicates which dbt models to run and any related configurations. The second refers to the dbt resource you’ll be using to run dbt.

  Let’s review what’s happening in this function in a bit more detail:

  * We use the `dbt` argument (which is a `DbtCliResource`) to execute a dbt command through its `.cli` method.
  * The `.stream()` method fetches the events and results of this dbt execution.
    * This is one of multiple ways to get the Dagster events, such as what models materialized or tests passed. We recommend starting with this and exploring other methods in the future as your use cases grow (such as fetching the run artifacts after a run). In this case, the above line will execute `dbt run`.
  *  The results of the `stream` are a Python generator of what Dagster events happened. We used [`yield from`](https://pythonalgos.com/generator-functions-yield-and-yield-from-in-python/) (not just `yield`!) to have Dagster track asset materializations.

  ### Updating the Definitions object

  The last step in setting up your dbt project in Dagster is adding the definitions you made (ex. your `dbt_resource` and `dbt_analytics` asset) to your code location’s Definitions object.

  Modify your root-level _`_init__.py `to:

  * Load assets from `dbt.py` file, and
  * Register the `dbt_resource` from `.resources` under the resource key `dbt`

  After making those changes, your root-level __init__.py should look like similar to below:
  ```
  from dagster import Definitions, load_assets_from_modules

  from .assets import trips, metrics, requests, dbt # Import the dbt assets
  from .resources import database_resource, dbt_resource # import the dbt resource
  # ...other existing imports

  # ... existing calls to load_assets_from_modules
  dbt_analytics_assets = load_assets_from_modules(modules=[dbt]) # Load the assets from the file

  # ... other declarations

  defs = Definitions(
      assets=[*trip_assets, *metric_assets, *requests_assets, *dbt_analytics_assets], # Add the dbt assets to your code location
      resources={
          "database": database_resource,
          "dbt": dbt_resource # register your dbt resource with the code location
      },
    # .. other definitions
  )
  ```

  ### Viewing dbt models in the Dagster UI

  You’re ready to see your dbt models represented as assets!

  Let's launch the dagster UI by running the following command and clicking "Open in New Tab" when the popup appears on the bottom right of your screen
  ```
  dagster dev
  ```

  1. Navigate to the Asset graph by clicking "Assets" on the top navigation bar
  2. Use the left asset graph window to expand the `default` group
  3. You should see your two dbt models, `stg_trips` and `stg_zones` converted as assets within your Dagster project!

  If you don't see the dbt models, click "Reload definitions" near the top right to have Dagster reload the code location.

  ![Dagster asset graph](https://dagster-university.vercel.app/images/dagster-dbt/lesson-3/asset-description-metadata.png)

  If you’re familiar with the Dagster metadata system, you’ll notice that the descriptions you defined for the dbt models in `staging.yml` are carried over as those for your dbt models. In this case, your `stg_zones`'s description would say *“The taxi zones, with enriched records and additional flags”*.

  And, of course, the orange dbt logo attached to the assets indicates that they are dbt models.

  Click the `stg_trips` node on the asset graph and look at the right sidebar. You’ll get some metadata out-of-the-box, such as the dbt code used for the model, how long the model takes to materialize over time, and the schema of the model.

  ### Running dbt models with Dagster

  After clicking around a bit and seeing the dbt models within Dagster, the next step is to materialize them.

  * Click the `stg_zones` asset.
  * Hold Control and click the `stg_trips` asset.
  * Click the "Materialize selected" button toward the top-right section of the asset graph.
  * Click the toast notification at the top of the page (or the hash that appears at the bottom right of a dbt asset’s node) to navigate to the run.
  * Under the run ID - in this case, `35b467ce` - change the toggle from a "timed view (stopwatch)" to the "flat view (blocks)".

  The run’s page should look similar to this:
  ![Run's page](https://dagster-university.vercel.app/images/dagster-dbt/lesson-3/dbt-run-details-page.png)

  Notice that there is only one “block,” or step, in this chart. That’s because Dagster runs dbt as it’s intended to be run: in a single execution of a `dbt` CLI command. This step will be named after the `@dbt_assets` -decorated asset, which we called `dbt_analytics` in the `assets/dbt.py` file.

  Scrolling through the logs, you’ll see the dbt commands Dagster executes, along with each model materialization. We want to point out two note-worthy logs.

  #### dbt commands
  ![dbt commands](https://dagster-university.vercel.app/images/dagster-dbt/lesson-3/dbt-logs-dbt-command.png)

  The log statement that indicates what dbt command is being run. Note that this executed the dbt run specified in the `dbt_analytics` asset.

  >💡 **What’s `--select fqn:*`?** As mentioned earlier, Dagster tries to run dbt in as few executions as possible. `fqn` is a [dbt selection method](https://docs.getdbt.com/reference/node-selection/methods#the-fqn-method) that is as explicit as it gets and matches the node names in a `manifest.json`. The `*` means it will run all dbt models.

  #### Materialization events

  ![materialization events](https://dagster-university.vercel.app/images/dagster-dbt/lesson-3/dbt-logs-materialization-events.png)

  The asset materialization events indicating that `stg_zones` and `stg_trips` were successfully materialized during the dbt execution.

  Try running just one of the dbt models and see what happens! Dagster will dynamically generate the `--select` argument based on the assets selected to run.
</details>

<details>
  <summary><h2>Lesson 3: Improving development</h2></summary>

  ### Speeding up the development cycle

  By now, you’ve had to run `dbt parse` to create the manifest file and reload your code location quite frequently, which doesn’t feel like the cleanest developer experience.

  Before we move on, we’ll reduce the number of steps in the feedback loop. We'll automate the creation of the manifest file by taking advantage of the `dbt_project` representation that we wrote earlier.

  #### Automating creating the manifest file in development

  The first detail is that the `dbt_project` doesn’t need to be part of an asset to be executed. This means that once a `dbt_project` is defined, you can use it to execute commands when your code location is being built. Rather than manually running `dbt parse`, let’s use the `dbt_project` to prepare the manifest file for us.
  ```
  dbt_project.prepare_if_dev()
  ```

  If you look at the dbt project’s `/target` directory, you’ll see it stores the artifacts. When you use `dagster dev` in local development and you reload your code, you'll see that a new manifest file is generated.

  The `prepare_if_dev()` method automatically prepares your dbt project at run time during development, meaning you no longer have to run dbt parse! The preparation process works by pulling the dbt project's dependencies and reloading the manifest file to detect any changes.

  Reload your code location in the Dagster UI, and you’ll see that everything should still work: the dbt models are still shown as assets and you can manually materialize any of the models.

  #### Creating the manifest for production

  This is great, however, it only handles the preparation of a new manifest file in local development. In production, where a dbt project is stable, we may want to prepare a new manifest file only at build time, during the deployment process. This can be done using the command line interface (CLI) available in the `dagster_dbt` package.

  Don't worry about the details for now! Later, we’ll discuss the details of how to create a manifest file programmatically during deployment using the `dagster_dbt` CLI.

  ### Debugging failed runs

  Data engineers spend more time debugging failures than writing new pipelines, so it’s important to know how to debug a failing dbt execution step.

  To demonstrate, we’re going to intentionally make a bug in our dbt model code, see it fail in Dagster, troubleshoot the failure, and then re-run the pipeline. Here, you’ll learn how to debug your dbt assets similarly to how you would troubleshoot dbt on its own.

  1. We'll add a new column called `zone_population` to the `select` statement in `stg_zones.sql`.
  ```
  with raw_zones as (
      select *
      from {{ source('raw_taxis', 'zones') }}
  )
  select
      zone_id,
      zone as zone_name,
      borough,
      zone_name like '%Airport' as is_airport,
      zone_population ## new column
  from raw_zones
  ```
  2. Navigate to the Dagster UI and reload the code location by clicking the "Reload Definitions" button.
  3. On the asset graph, locate the `stg_zones` asset. You’ll see a yellow "Code version" tag indicating that Dagster recognized the SQL code changed.
  4. Select the `stg_zones` asset and click the "Materialize" button.
  5. Navigate to the run’s details page.
  6. On the run’s details page, click the `dbt_analytics` step.
  7. To view the logs, click the `stdout` button on the top-left of the pane.

  In these logs, we can see that DuckDB can’t find the `zone_population` column in `stg_zones`. That’s because this column doesn’t exist!

  Now that we know what the problem is let’s fix it.

  1. Remove the `zone_population` column from the `stg_zones` model
  2. In the Dagster UI, reload the code location to allow Dagster to pick up the changes.

  At this point, if you materialize the `stg_zones` asset again, the run should be successful
</details>

<details>
  <summary><h2>Lesson 4: Adding dependencies and automations to dbt models</h2></summary>

  ### Connecting dbt models to Dagster assets

  You may have noticed that the sources for your dbt projects are not just tables that exist in DuckDB, but also assets that Dagster created. However, the staging models (`stg_trips`, `stg_zones`) that use those sources aren’t linked to the Dagster assets (`taxi_trips`, `taxi_zones`) that produced them.

  Let’s fix that by telling Dagster that the dbt sources are the tables that the `taxi_trips` and `taxi_zones` asset definitions produce. To match up these assets, we'll override the dbt assets' keys. By having the asset keys line up, Dagster will know that these assets are the same and should merge them.

  This is accomplished by changing the dbt source’s asset keys to be the same as the matching assets that Dagster makes. In this case, the dbt source’s default asset key is `raw_taxis/trips`, and the table that we’re making with Dagster has an asset key of `taxi_trips`.

  To adjust how Dagster names the asset keys for your project’s dbt models, we’ll need to override the `dagster-dbt` integration’s default logic for how to interpret the dbt project. This mapping is contained in the `DagsterDbtTranslator` class.

  #### Customizing how Dagster understands dbt projects

  The `DagsterDbtTranslator` class is the default mapping for how Dagster interprets and maps your dbt project. As Dagster loops through each of your dbt models, it will execute each of the translator’s functions and use the return value to configure your new Dagster asset.

  However, you can override its methods by making a new class that inherits from and provides your logic for a dbt model. Refer to the `dagster-dbt` package’s [API Reference](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.DagsterDbtTranslator) for more info on the different functions you can override in the `DagsterDbtTranslator` class.

  For now, we’ll customize how asset keys are defined by overriding the translator’s `get_asset_key` method.

  Open the `assets/dbt.py`
  1. Update imports
     ```
     from dagster import AssetKey
     from dagster_dbt import DagsterDbtTranslator
     ```
  2. Create a new class called `CustomizedDagsterDbtTranslator` that inherits from the `DagsterDbtTranslator`.
     ```
     class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
     ```
  3. In this class, create a method called `get_asset_key`.

     This is a method of `DagsterDbtTranslator` class that we'll override and customize to do as we need. It is an instance method, so we'll have its first argument be `self`, to follow [Pythonic](https://builtin.com/software-engineering-perspectives/python-guide) conventions. The second argument refers to a dictionary/JSON object for the dbt model’s properties, which is based on the manifest file from earlier. Let’s call that second argument `dbt_resource_props`. The return value of this function is an object of the `AssetKey` class.
     ```
     def get_asset_key(self, dbt_resource_props):
     ```
  4. There are two properties that we’ll want from `dbt_resource_props`: the `resource_type` (ex., model, source, seed, snapshot) and the `name`, such as `trips` or `stg_trips`. Access both of those properties from the `dbt_resource_props` argument and store them in their own respective variables (`type` and `name`):
     ```
     resource_type = dbt_resource_props["resource_type"]
     name = dbt_resource_props["name"]
     ```
  5. As mentioned above, the asset keys of our existing Dagster assets used by our dbt project are named `taxi_trips` and `taxi_zones`. If you were to print out the `name`, you’d see that the dbt sources are named `trips` and `zones`. Therefore, to match our asset keys up, we can prefix our keys with the string `taxi_`.

     You have full control over how each asset can be named, as you can define how asset keys are created. In our case we only want to rename the dbt sources, but we can keep the asset keys of the models the same.

     The object-oriented pattern of the `DagsterDbtTranslator` means that we can leverage the existing implementations of the parent class by using the `super` method. We’ll use this pattern to customize how the sources are defined but default to the original logic for deciding the model asset keys.
     ```
     if resource_type == "source":
        return AssetKey(f"taxi_{name}")
     else:
        return super().get_asset_key(dbt_resource_props)
     ```

  ### Creating assets that depend on dbt models

  At this point, you’ve loaded your dbt models as Dagster assets and linked the dependencies between the dbt assets and their source Dagster assets. However, a dbt model is typically not the last asset in a pipeline. For example, you might want to:

  Generate a chart,
  Update a dashboard, or
  Send data to Salesforce
  In this section, you’ll learn how to do this by defining a new Dagster asset that depends on a dbt model. We’ll make some metrics in a dbt model and then use Python to generate a chart with that data.

  If you’re familiar with New York City, you might know that there are three major airports - JFK, LGA, and EWR - in different parts of the metropolitan area. Let's say you’re curious if a traveler's final destination impacts the airport they fly into. For example, how many people staying in Queens flew into LGA?

  #### Creating the dbt model

  To answer these questions, let’s define a new dbt model that builds a series of metrics from the staging models you wrote earlier.

  In the `analytics/models` directory:

  1. Create a new directory called `marts`.
  2. Create a new file called `marts/location_metrics.sql`.
  3. Insert code:
     ```
     with
         trips as (
             select *
             from {{ ref('stg_trips') }}
         ),
         zones as (
             select *
             from {{ ref('stg_zones') }}
         ),
         trips_by_zone as (
             select
                 pickup_zones.zone_name as zone,
                 dropoff_zones.borough as destination_borough,
                 pickup_zones.is_airport as from_airport,
                 count(*) as trips,
                 sum(trips.trip_distance) as total_distance,
                 sum(trips.duration) as total_duration,
                 sum(trips.total_amount) as fare,
                 sum(case when duration > 30 then 1 else 0 end) as trips_over_30_min
             from trips
             left join zones as pickup_zones on trips.pickup_zone_id = pickup_zones.zone_id
             left join zones as dropoff_zones on trips.dropoff_zone_id = dropoff_zones.zone_id
             group by all
         )
     select *
     from trips_by_zone
     ```
  4. In the Dagster UI, reload the code location.
  5. Observe and materialize the new `location_metrics` dbt asset:

  #### Creating the Dagster asset

  Next, we’ll create an asset that uses some of the columns in the `location_metrics` model to chart the number of taxi trips that happen per major NYC airport and the borough they come from.

  ##### Adding a new constant

  Let's start by adding a new string constant to reference when building the new asset. This will make it easier for us to reference the correct location of the chart in the asset.

  In the `assets/constants.py` file, add the following to the end of the file:
  ```
  AIRPORT_TRIPS_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "airport_trips.png"))
  ```

  This creates a path to where we want to save the chart. The `get_path_for_env` utilty function is not specific to Dagster, but rather is a utility function we've defined in this file to help with Lesson 7 (Deploying your Dagster and dbt project).

  ##### Creating the airport_trips asset

  Now we’re ready to create the asset!

  1. Open the `assets/metrics.py` file.
  2. At the end of the file, define a new asset called `airport_trips` with the existing `DuckDBResource` named `database` and it will return a `MaterializeResult`, indicating that we'll be returning some metadata:
     ```
     def airport_trips(database: DuckDBResource) -> MaterializeResult:
     ```
  3. Add the asset decorator to the airport_trips function and specify the location_metrics model as a dependency:
     ```
     @asset(
         deps=["location_metrics"],
     )
     ```
     > **Note**: Because Dagster doesn’t discriminate and treats all dbt models as assets, you’ll add this dependency just like you would with any other asset.

  4. Fill in the body of the function with the following code to follow a similar pattern to your project’s existing pipelines: query for the data, use a library to generate a chart, save the chart as a file, and embed the chart.
     ```
     """
         A chart of where trips from the airport go
     """

     query = """
         select
            zone,
            destination_borough,
             trips
         from location_metrics
         where from_airport
     """
     with database.get_connection() as conn:
         airport_trips = conn.execute(query).fetch_df()

     fig = px.bar(
         airport_trips,
         x="zone",
         y="trips",
         color="destination_borough",
         barmode="relative",
         labels={
             "zone": "Zone",
             "trips": "Number of Trips",
             "destination_borough": "Destination Borough"
         },
     )

     pio.write_image(fig, constants.AIRPORT_TRIPS_FILE_PATH)

     with open(constants.AIRPORT_TRIPS_FILE_PATH, 'rb') as file:
         image_data = file.read()

     # Convert the image data to base64
     base64_data = base64.b64encode(image_data).decode('utf-8')
     md_content = f"![Image](data:image/jpeg;base64,{base64_data})"

     return MaterializeResult(
         metadata={
             "preview": MetadataValue.md(md_content)
         }
     )
     ```
  Reload your code location to see the new `airport_trips` asset within the `metrics` group. Notice how the asset graph links the dependency between the `location_metrics` dbt asset and the new `airport_trips` chart asset:

  ### Automating dbt models in Dagster

  Did you realize that your dbt models have already been scheduled to run on a regular basis because of an existing schedule within this Dagster project?

  Check it out in the Dagster UI by clicking "Overview" in the top navigation bar, then the Jobs tab. Click `trip_update_job` to check out the job’s details. It looks like the dbt models are already attached to this job!

  Pretty cool, right? Let’s check out the code that made this happen.
  Open the `dagster_university/jobs/__init__.py` and look at the definition for `trip_update_job`

  The dbt models were included in this job because of the `AssetSelection.all()` call. This reinforces the idea that once you load your dbt project into your Dagster project, Dagster will recognize and treat all of your dbt models as assets.

  #### Excluding specific dbt models

  Treating dbt models as assets is great, but one of the core tenets of Dagster’s dbt integration is respecting how dbt is used, along with meeting dbt users where they are. That’s why there are a few utility methods that should feel familiar to dbt users. Let’s use one of these methods to remove some of our dbt models from this job explicitly.

  Pretend that you’re working with an analytics engineer, iterating on the `stg_trips` model and planning to add new models that depend on it soon. Therefore, you’d like to exclude `stg_trips` and any new hypothetical dbt models downstream of it until the pipeline stabilizes. The analytics engineer you’re working with is really strong with dbt, but not too familiar with Dagster.

  This is where you’d lean on a function like [`build_dbt_asset_selection`](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.build_dbt_asset_selection). This utility method will help your analytics engineer contribute without needing to know Dagster’s asset selection syntax. It takes two arguments:

  * A list of `@dbt_assets` definitions to select models from
  * A string of the selector using [dbt’s selection syntax](https://docs.getdbt.com/reference/node-selection/syntax) of the models you want to select

  The function will return an `AssetSelection` of the dbt models that match your dbt selector. Let’s put this into practice:

  1. At the top of `jobs/__init__.py` insert
     ```
     from ..assets.dbt import dbt_analytics
     from dagster_dbt import build_dbt_asset_selection
     ```
  2. After the other selections, define a new variable called `dbt_trips_selection`
     ```
     dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips+")
     ```
  3. Update the `selection` argument in the `trip_update_job` to subtract the `dbt_trips_selection`
     ```
     trip_update_job = define_asset_job(
         name="trip_update_job",
         partitions_def=monthly_partition,
         selection=AssetSelection.all() - trips_by_week - adhoc_request - dbt_trips_selection
     )
     ```
  4. Reload the code location and confirm that the dbt models are not in the `trip_update_job` anymore!

  You might notice that the `airport_trips` asset is still scheduled to run with this job! That’s because the `build_dbt_asset_selection` function only selects **dbt models** and not Dagster assets.

  If you want to also exclude the new `airport_trips` asset from this job, modify the `dbt_trips_selection` to include all **downstream assets**, too. Because we’re using Dagster’s native functionality to select all downstream assets, we can now drop the `+` from the dbt selector:
  ```
  dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips").downstream()
  ```

  Reload the code location and look at the `trip_update_job` once more to verify that everything looks right.

  > 💡 Want an even more convenient utility to do this work for you? Consider using the similar [`build_schedule_from_dbt_selection`](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.build_schedule_from_dbt_selection) function to quickly create a job and schedule for a given dbt selection.
</details>

<details>
  <summary><h2>Lesson 5: Partitioning dbt models</h2></summary>

  ### Creating an incremental model

  As mentioned, partitions don’t **replace** incremental models, but you’ll soon see how you can expand their functionality by partitioning them. In fact, we’ll first write an incremental dbt model and then show you how to use Dagster to partition it.

  This model will be a series of stats about all New York taxi trips. It would be expensive to compute this every day because of the granularity of the metrics and the fact that some of the measures are computationally expensive to calculate. Therefore, this model will be incremental.

  In your dbt project, create a new file `analytics/models/marts/daily_metrics.sql`
  ```
  {{
    config(
      materialized='incremental',
      unique_key='date_of_business'
    )
  }}

  with
      trips as (
          select *
          from {{ ref('stg_trips') }}
      ),
      daily_summary as (
          select
              date_trunc('day', pickup_datetime) as date_of_business,
              count(*) as trip_count,
              sum(duration) as total_duration,
              sum(duration) / count(*) as average_duration,
              sum(total_amount) as total_amount,
              sum(total_amount) / count(*) as average_amount,
              sum(case when duration > 30 then 1 else 0 end) / count(*) as pct_over_30_min
          from trips
          group by all
      )
  select *
  from daily_summary
  {% if is_incremental() %}
      where date_of_business > (select max(date_of_business) from {{ this }})
  {% endif %}
  ```

  ### Creating a partitioned dbt asset

  We’ve built the foundation on the dbt side, and now we can make the appropriate changes on the Dagster side. We’ll refactor our existing Dagster code to tell dbt that the incremental models are partitioned and what data to fill in.

  We want to configure some of these models (the incremental ones) with partitions. In this section, we’ll show you a use case that has multiple `@dbt_assets` definitions.

  To partition an incremental dbt model, you’ll need first to partition your `@dbt_assets` definition. Then, when it runs, we’ll figure out what partition is running and tell dbt what the partition’s range is. Finally, we’ll modify our dbt model only to insert the records found in that range.

  #### Defining an incremental selector

  We have a few changes to make to our dbt setup to get things working. In `assets/dbt.py`:

  1. Add the following imports to the top of the file:
     ```
     from ..partitions import daily_partition
     import json
     ```
     This imports the `daily_partition` from `dagster_university/partitions/__init__.py` and the `json` standard module. We’ll use the `json` module to format how we tell dbt what partition to materialize.
  2. We now need a way to indicate that we’re selecting or excluding incremental models, so we’ll make a new constant in the `dbt.py` file called `INCREMENTAL_SELECTOR`
     ```
     INCREMENTAL_SELECTOR = "config.materialized:incremental"
     ```
     This string follows dbt’s selection syntax to select all incremental models. In your own projects, you can customize this to select only the specific incremental models that you want to partition.

  ### Creating a new @dbt_assets function

  Previously, we used the `@dbt_assets` decorator to say *“this function produces assets based on this dbt project”*. Now, we also want to say *“this function produces partitioned assets based on a selected set of models from this dbt project.”* We’ll write an additional `@dbt_assets`-decorated function to express this.

  In `dagster_university/assets/dbt.py`, define another `@dbt_assets` function below the original one. Name it `dbt_incremental_models` and have it use the same manifest that we’ve been using. Also, add arguments to specify which models to select (`select`) and what partition (`partitions_def`) to use:
  ```
  @dbt_assets(
      manifest=dbt_project.manifest_path,
      dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
      select=INCREMENTAL_SELECTOR,     # select only models with INCREMENTAL_SELECTOR
      partitions_def=daily_partition   # partition those models using daily_partition
  )
  def incremental_dbt_models(
      context: AssetExecutionContext,
      dbt: DbtCliResource
  ):
    yield from dbt.cli(["build"], context=context).stream()
  ```
  This tells the function to only select models with `INCREMENTAL_SELECTOR` and to partition them using the `daily_partition`.

  ### Partitioning the incremental_dbt_models function

  Now that the `@dbt_assets` definition has been created, it's time to fill in its body. We’ll start by using the context argument, which contains metadata about the Dagster run.

  One of these pieces of information is that we can fetch **the partition this execution is trying to materialize**! In our case, since it’s a time-based partition, we can get the **time window** of the partitions we’re materializing, such as `2023-03-04T00:00:00+00:00` to `2023-03-05T00:00:00+00:00.`

  First, add the following to the `@dbt_assets` function body, before the `yield`:
  ```
  time_window = context.partition_time_window
  dbt_vars = {
      "min_date": time_window.start.strftime('%Y-%m-%d'),
      "max_date": time_window.end.strftime('%Y-%m-%d')
  }
  ```
  This fetches the time window and stores it as a variable (`time_window`) so we can use it later.

  Now that we know *what* partitions we’re executing, the next step is to tell dbt the partition currently being materialized. To do that, we’ll take advantage of dbt’s `vars` argument to pass this information at runtime. Because the `dbt.cli` function has the same capabilities as the `dbt` CLI, we can dynamically set the arguments we pass into it. To communicate this time window, we’ll pass in a `min_date` and `max_date` variable. Update the `yield` in the `@dbt_assets` definition to the following:
  ```
  yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
  ```

  ### Updating the dbt_analytics function

  Now that you have a dedicated `@dbt_assets` definition for the incremental models, you’ll need to **exclude** these models from your original dbt execution.

  Modify the `dbt_analytics` definition to exclude the `INCREMENTAL_SELECTOR`:
  ```
  @dbt_assets(
      manifest=dbt_project.manifest_path,
      dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
      exclude=INCREMENTAL_SELECTOR, # Add this here
  )
  def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
      yield from dbt.cli(["build"], context=context).stream()
  ```

  ### Updating the daily_metrics model

  Finally, we’ll modify the `daily_metrics.sql` file to reflect that dbt knows what partition range is being materialized. Since the partition range is passed in as variables at runtime, the dbt model can access them using the `var` dbt macro.

  In `analytics/models/marts/daily_metrics.sql`, update the contents of the model's incremental logic (`% if is_incremental %`}) to the following:
  ```
  where date_of_business between '{{ var('min_date') }}' and '{{ var('max_date') }}'
  ```

  Here, we’ve changed the logic to say that we only want to select rows between the `min_date` and the `max_date`.

  ### Running the pipeline

  That’s it! Now you can check out the new `daily_metrics` asset in Dagster.

  1. In the Dagster UI, reload the code location. Once loaded, you should see the new partitioned daily_metrics asset
  2. Click the `daily_metrics` asset and then the "Materialize selected" button. You’ll be prompted to select some partitions first.
  3. Once the run starts, navigate to the run’s details page to check out the event logs. The executed dbt command should look something like this:
  ```
  dbt build --vars {"min_date": "2023-03-04T00:00:00+00:00", "max_date": "2023-03-05T00:00:00+00:00"} --select config.materialized:incremental
  ```
</details>
