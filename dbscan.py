# -*- coding: utf-8 -*-
import psycopg2
from Point import Point
import csv

CLUSTER_ID = 0
NOISE = 0
UNCLASSIFIED = -1


def update_dbs(set_of_points, dry_run=False, sql_dump=False, csv_dump=False):
    """Performs the update of the rows in the database. It interact directly with Postgres performing the UPDATE queries.
    Optional parameters allow to dump the Sql statements and/or to save the list of points in CSV format.
    Parameters:
        set_of_points: List of Points.
        dry_run: Boolean. Optional parameter. If True enable the dry run. The update is not executed.
        sql_dump: Boolean. Optional parameter. If True enable the dump of the UPDATE queries into dump.sql file.
        csv_dump: Boolean. Optional parameter. If True enable the dump of the list of points in a CSV file.
    """
    queries = ""
    for gid, point in set_of_points.items():
        query = "UPDATE animale.animal SET label = " + str(point.label) + " WHERE gid=" + str(gid) + ";\n"
        queries += query

    if not dry_run:
        print("UPDATE on DB")
        cur.execute(queries)
        conn.commit()

    if sql_dump:
        with open("dump.sql", 'w') as f:
            f.write(queries)
        print("dump.sql has been written.")

    if csv_dump:
        cur.execute('''SELECT * FROM animale.animal;''')
        original_rows = cur.fetchall()

        with open("dump.csv", 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['gid', 'animal', 'time', 'geom', 'label'])
            for row in original_rows:
                tmp = list(row)
                gid = tmp[0]
                tmp[-1] = set_of_points[gid].label
                writer.writerow(tmp)
        print("dump.csv has been written")


def region_query(set_of_points, p, eps):
    """Returns the list of points having distance less then eps from point p.
    Parameters:
        set_of_points: List of Points.
        p: Point. Reference point.
        eps: Integer. Distance.
    """
    sql_query = "select a.gid, a.label " \
                "from animale.animal as a, (select * from animale.animal where gid = " + str(p.gid) + ") as subqry " \
                "where a.gid != subqry.gid and ST_dwithin(subqry.geom, a.geom, " + str(eps) + ");"
    cur.execute(sql_query)
    rows = cur.fetchall()
    query_result = {}
    for row in rows:
        new_p = Point(row)
        query_result[new_p.gid] = set_of_points[new_p.gid]
    return query_result


def change_cluster_id(set_of_points, seeds, cluster_id):
    """Change the cluster id of all points belonging to seeds in set_of_points.
    Parameters:
        set_of_points: List of Points.
        seeds: List of Points.
        cluster_id: Integer. New cluster Id.
    """
    for seed_key in seeds.keys():
        set_of_points[seed_key].label = cluster_id


def expand_cluster(set_of_points, p, cluster_id, eps, minpts):
    """Recursively classify the points in the neighborhood of p.
    Parameters:
        set_of_points: List of Points.
        p: Point. Reference Point.
        cluster_id: Integer. Cluster id of point p.
        eps: Integer. Radius of the neighborhood.
        minpts: Integer. Minimum number of points for the cluster.
    """
    seeds = region_query(set_of_points, p, eps)
    if len(seeds) < minpts: # this is not a core point
        set_of_points[p.gid].label = NOISE
        return False
    else: # all point in the seeds set are density reachable from p.
        change_cluster_id(set_of_points, seeds, cluster_id)

        try:
            del seeds[p.gid]
        except KeyError:
            pass # it will fail because the query doesn't get the point p.

        while len(seeds.keys()) != 0:
            current_point = seeds[list(seeds.keys())[0]]
            result = region_query(set_of_points, current_point, eps)

            if len(result) >= minpts:
                for key in result.keys():
                    result_point = result[key]
                    if result_point.label == UNCLASSIFIED or result_point.label == NOISE:
                        if result_point.label == UNCLASSIFIED:
                            seeds[result_point.gid] = result_point
                        change_cluster_id(set_of_points, {result_point.gid:result_point},cluster_id)

            del seeds[current_point.gid]
        return True


def next_id():
    """Returns the next cluster id."""
    global CLUSTER_ID
    CLUSTER_ID = CLUSTER_ID + 1
    return CLUSTER_ID


def dbscan(set_of_points, eps, minpts):
    """ Main function of the dbscan algorithm,
    Parameter:
        set_of_points: List of Points. List of points to be classified.
        eps: Integer.
        minpts: Integer.
    """
    cluster_id = next_id()
    for key in set_of_points.keys():
        p = set_of_points[key]
        if p.label == -1:
            if expand_cluster(set_of_points, p, cluster_id, eps, minpts):
                print("Found cluster = {}".format(cluster_id))
                cluster_id = next_id()

    # dry_run=[True|False], sql_dump=[True|False], csv_dump=[True|False]
    update_dbs(set_of_points, dry_run=False, sql_dump=True, csv_dump=True)


if __name__ == '__main__':
    conn = psycopg2.connect(database="gis", user="emanuele", password="xxx", host="127.0.0.1", port="5432")
    cur = conn.cursor()

    cur.execute('''UPDATE animale.animal SET label = -1;''')
    conn.commit()

    cur.execute('''SELECT gid, label FROM animale.animal;''')
    rows = cur.fetchall()
    set_of_points = {}
    for row in rows:
        p = Point(row)
        set_of_points[p.gid] = p

    eps = 300
    minpts = 20
    dbscan(set_of_points, eps, minpts)

    conn.close()