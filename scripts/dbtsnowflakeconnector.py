def parse_yaml_file(file):
    """
    Accepts a yaml file name and safe parses to a Python dict
    """
    with open(file, 'r') as cfgyml:
        dict = yaml.safe_load(cfgyml)
    return dict

def get_target_config(target: str):
    """
    Accepts a target name and parses dbt config files for properties
    """

    # Get working directory locations
    script_path = Path(__file__).resolve() # Absolute path to script being executed
    script_root = Path(script_path).parent.absolute() # Script root directory
    project_root = Path(script_root).parent.absolute() # Parent of scripts directory
    project_config_location = project_root / "dbt_project.yml" # Absolute path of dbt_project
    user_home = Path.home() # Absolute path of user home
    dbt_profiles_location = user_home / ".dbt" / "profiles.yml" # Absolute path of dbt Profiles

    # Parse project config
    project_cfg = parse_yaml_file(project_config_location)

    # Parse profiles config
    profiles_cfg = parse_yaml_file(dbt_profiles_location)


    # Get profile name from project config
    profile_name = project_cfg.get('profile')

    # Get target details from profiles config for the profile specified in project config
    target_name = target.lower()
    target_dict = profiles_cfg[profile_name]['outputs'][target_name]

    return target_dict



def create_connection(target_properties):
    tp = target_properties
    
    # Declare class to hold snowflake config properties
    class snowflake_config:
        def __init__(self,
                    authenticator=tp['authenticator'],
                    account=tp['account'],
                    database=tp['database'],
                    schema=tp['schema'],
                    warehouse=tp['warehouse'],
                    role=tp['role'],
                    user=tp['user']
                    ):
            self.authenticator = tp['authenticator']
            self.account = tp['account']
            self.database = tp['database']
            self.schema = tp['schema']
            self.warehouse = tp['warehouse']
            self.role = tp['role']
            self.user = tp['user']

        def __str__(self):
            return f"snowflake_config({self.authenticator},{self.account},{self.database},{self.schema},{self.warehouse},{self.role},{self.user})"


    # Instantiate new config object for easy access to attributes
    sc = snowflake_config()

    # Create a new Snowflake connection
    con = snowflake.connector.connect(
        authenticator=sc.authenticator,
        account=sc.account,
        warehouse=sc.warehouse,
        database=sc.database,
        schema=sc.schema,
        user=sc.user
    )

    return con
    
    
    
def get_query_results_as_list(connection, sql: str):
    """
    Accepts SnowflakeConnection object and sql statement
    """
    # Run the statement with connection management
    with connection:
        cur = connection.cursor(DictCursor)
        cur.execute(sql)

    # Get rows as a list of dictionaries
    return cur.fetchall()
