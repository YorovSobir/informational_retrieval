from ranking import BM25
from utils.db_service import DBService
from utils.utils import build_parser


class Find:
    def __init__(self, db_service, path_to_ind, path_to_doc):
        self.bm25 = BM25(path_to_ind, path_to_doc)
        self.db = db_service.db
        self.cur = self.db.cursor()
        cmd = """
            SELECT
                id_document
            FROM disease_treatment
        """
        self.cur.execute(cmd)
        self.docs_disease = set([x[0] for x in self.cur.fetchall()])

    def cards(self, query):
        docs = self.bm25.ranked_docs(query)
        result = []
        for doc in docs:
            if doc in self.docs_disease:
                cmd = """
                        SELECT
                          ST.url,
                          DT.disease,
                          DT.treatment,
                          T.id
                        FROM (SELECT
                                D.val AS d_val,
                                L.id AS id
                              FROM disease_levels AS DL
                                JOIN disease AS D ON DL.id_disease = D.id
                                JOIN levels AS L ON DL.id_level = L.id) AS T
                          RIGHT JOIN disease_treatment AS DT ON LOWER(DT.disease) LIKE concat('%', LOWER(T.d_val), '%')
                           JOIN storage as ST ON ST.id = DT.id_document WHERE DT.id_document = {0}
                      """.format(doc)
                self.cur.execute(cmd)
                res = self.cur.fetchall()
                if res:
                    result.append(res)
            if len(result) >= 10:
                break
        return result

    def ranked_list(self, query):
        return self.bm25.ranked_docs(query)


parser = build_parser()
args = parser.parse_args()


def cards(query):
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    f = Find(db_service, args.index_dir, args.data_dir)
    return f.cards(query)


def ranked_list(query):
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    f = Find(db_service, args.index_dir, args.data_dir)
    return f.ranked_list(query)
