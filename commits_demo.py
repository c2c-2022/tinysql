import table_maker as TM
from datetime import datetime


t1 = datetime(year=1980, month=12, day=15)
t2 = datetime(year=1980, month=12, day=16)

# TABLES -- global scope
REPO, COMMITTER, COMMIT_REPO = ['' for x in range(3)]

def main():
    repo = TM.TableLoader(table_data=str_to_table(REPO), table_name='repo')
    committer = TM.TableLoader(table_data=str_to_table(COMMITTER), table_name='committer')
    repo_commit = TM.TableLoader(table_data=str_to_table(COMMIT_REPO), table_name='repo_commit')

    SQL = """
    SELECT committer.name, repo.name, repo_commit.date,
           ROW_NUMBER() OVER (PARTITION BY repo.id order by repo_commit.date) as RN  
    FROM repo_commit
    JOIN committer on 
         repo_commit.committer = committer.id
    JOIN repo 
         ON repo_commit.repo = repo.id
    """

    # who committed to the most repositories?
    SQL_2 = """
    WITH DISTINCT_COMMITS AS (
    SELECT DISTINCT REPO, COMMITTER
    FROM REPO_COMMIT
    ),
    COUNTED AS (
    SELECT  COMMITTER, COUNT(*) AS HOW_MANY
    FROM DISTINCT_COMMITS
    GROUP BY COMMITTER
    ),
    RN AS (
    SELECT COMMITTER, HOW_MANY, 
    ROW_NUMBER() OVER (ORDER BY HOW_MANY) AS RN  
    FROM COUNTED
    )
    SELECT * FROM RN 
    """
    result = repo.run_select(SQL_2)
    for x in result:
        print(x)

def str_to_table(input_string:  str, delim='|'):
    retval = list()
    # remove leading/trailing white space because it is convenient  for user
    # to allow that in formatting
    input_string = input_string.strip()
    for line in input_string.split(sep='\n'):
        fields = [None if x.strip() == '' else x.strip() for x in line.split(delim)]
        retval.append(fields)
    return  retval

def define_data():
    global REPO, COMMITTER, COMMIT_REPO

    REPO = """
    ID|NAME|HOST|IS_ACTIVE
    1|AggregatorX|atlassian|True
    2|markdown|github|False
    3|markdown2|gitlab|True
    8|new-commerce|github|True
    """

    COMMITTER = """
    ID|NAME
    101|Archduke
    105|radiant marmalade
    110|coder12001
    """

    COMMIT_REPO = f"""
    REPO|COMMITTER|DATE
    1|105|{t1}
    1|105|{t2}
    2|110|{t1}
    2|110|{t2}
    2|105|{t1}
    8|105|{t1}
    """
# this file will never be imported it is for  interactive use.
# so the <if __main__...> construct would be unhelpful
define_data()
main()
