# -*- coding: utf-8 -*-
import psycopg2
from Point import Point

CLUSTER_ID = 0
NOISE = 0
UNCLASSIFIED = -1


def update_dbs(set_of_points):
    sql_queryes = ""
    for gid, point in set_of_points.items():
        sql_queryes = sql_queryes + ("UPDATE animale.animal SET label = " + str(point.label) + " WHERE gid=" + str(gid) + ";\n")
    cur.execute(sql_queryes)
    print("UPDATE on DB")


def region_query(set_of_points, p, eps):
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
    for seed_key in seeds.keys():
        set_of_points[seed_key].label = cluster_id


def expand_cluster(set_of_points, p, cluster_id, eps, minpts):
    seeds = region_query(set_of_points, p, eps)
    if len(seeds) < minpts: # this is not a core point
        set_of_points[p.gid].label = NOISE
        return False
    else: # all point in the seeds set are density reachable from p.
        change_cluster_id(set_of_points, seeds, cluster_id)

        try:
            del seeds[p.gid]
        except KeyError:
            pass

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
    global CLUSTER_ID
    CLUSTER_ID = CLUSTER_ID + 1
    return CLUSTER_ID


def dbscan(set_of_points, eps, minpts):
    """
    :param set_of_points: List of Points
    :param eps:
    :param minpts:
    :return:
    """
    cluster_id = next_id()
    for key in set_of_points.keys():
        p = set_of_points[key]
        if p.label == -1:
            if expand_cluster(set_of_points, p, cluster_id, eps, minpts):
                print("Found cluster = {}".format(cluster_id))
                cluster_id = next_id()

    update_dbs(set_of_points)

if __name__ == '__main__':
    conn = psycopg2.connect(database="gis", user="postgres", password="AkrasiaPostgres", host="127.0.0.1", port="5432")
    cur = conn.cursor()
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