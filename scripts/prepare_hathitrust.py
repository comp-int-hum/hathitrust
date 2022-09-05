import gzip
import re
import uuid
import sys
import shutil
import logging
import csv
import zipfile
import json
from hashlib import md5
import hashlib
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, \
    FOAF, ODRL2, ORG, OWL, PROF, PROV, RDF, RDFS, SDO, SH, \
    SKOS, SOSA, SSN, TIME, VOID, XMLNS, XSD
from pairtree import PairtreeStorageFactory


publisher_ids, author_ids = {}, {}

csv.field_size_limit(sys.maxsize)

CDH = Namespace("http://cdh.jhu.edu/materials/")

logging.basicConfig(level=logging.INFO)


def partial(gr, fname, authors, publishers):
    logging.info("Preparing data for writing to %s", fname)
    for (first, last, birth_year, death_year), aid in authors.items():
        if first and last:
            gr.add(
                (
                    CDH[aid],
                    RDF.type,
                    SDO.Person
                )
            )
            gr.add(
                (
                    CDH[aid],
                    SDO.givenName,
                    Literal(first)
                )
            )
            gr.add(
                (
                    CDH[aid],
                    SDO.familyName,
                    Literal(last)
                )
            )
            if birth_year:
                gr.add(
                    (
                        CDH[aid],
                        SDO.birthDate,
                        Literal("{}-01-01".format(birth_year), datatype=XSD.date)
                    )
                )
            if death_year:
                gr.add(
                    (
                        CDH[aid],
                        SDO.deathDate,
                        Literal("{}-01-01".format(death_year), datatype=XSD.date)
                    )
                )
                
        elif last:
            gr.add(
                (
                    CDH[aid],
                    SDO.name,
                    Literal(last)
                )
            )
        else:
            print("Odd author entry: {} {} {} {}".format(first, last, birth, death))
        
    for (name, place), pid in publishers.items():
        gr.add(
            (
                CDH[pid],
                SDO.name,
                Literal(name)
            )
        )

        gr.add(
            (
                CDH[pid],
                RDF.type,
                SDO.Organization
            )
        )
        
        gr.add(
            (
                CDH[pid],
                SDO.location,
                Literal(place)
            )
        )
    gr.serialize(destination=fname)



def process_shapes(graph, shapes):

    def q(v):
        return v if isinstance(v, (URIRef, BNode, Literal)) else CDH[v]
    
    for name, attrs in shapes.items():
        shape = q("{}Shape".format(name))
        graph.add(
            (
                shape,
                SH.closed,
                Literal(True)
            )
        )
        graph.add(
            (
                q(shape),
                q(RDF.type),
                q(SH.NodeShape)
            )
        )
        graph.add(
            (
                q(shape),
                q(SH.targetClass),
                q(name)
            )
        )
        for i, (attr_name, attr_vals) in enumerate(attrs.items()):
            b = BNode()
            graph.add(
                (
                    q(shape),
                    q(SH.property),
                    q(b)
                )
            )
            graph.add(
                (
                    q(b),
                    q(SH.path),
                    q(attr_name)
                )
            )
            for k, v in attr_vals:
                graph.add(
                    (
                        q(b),
                        q(k),
                        q(v)
                    )
                )


if __name__ == "__main__":

    import argparse
    from glob import glob
    import os.path
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_input", dest="csv_input")
    parser.add_argument("--hathitrust_path", dest="hathitrust_path")
    parser.add_argument("--materials_output", dest="materials_output")
    parser.add_argument("--annotation_output", dest="annotation_output")
    parser.add_argument("--data_output", dest="data_output")
    parser.add_argument("--schema_output", dest="schema_output")
    args = parser.parse_args()

    g = Graph()
    
    schema_graph = Graph()
    schema_graph.bind("cdh", CDH)
    shapes = {
        "Author" : {
            SDO.familyName : [(SH.datatype, XSD.string)],
            SDO.givenName : [(SH.datatype, XSD.string)],
            SDO.birthDate : [
                (SH.datatype, XSD.date),
                (SH.lessThan, SDO.deathDate)
            ],
            SDO.deathDate : [(SH.datatype, XSD.date)],
        },
        "Document" : {
            SDO.creator : [(SH["class"], CDH["Author"])],
            SDO.contentUrl : [(SH.datatype, XSD.string)],
            SDO.name : [(SH.datatype, XSD.string)],
            SDO.inLanguage : [(SH.datatype, XSD.string)],
            SDO.datePublished : [(SH.maxInclusive, Literal("2022", datatype=XSD.date))],
            SDO.publisher : [(SH["class"], CDH["Publisher"])],
            SDO.position : [(SH.datatype, XSD.string)],
        },
        "Publisher" : {
            SDO.name : [(SH.datatype, XSD.string)],
            SDO.location : [(SH.datatype, XSD.string)],
        }
    }
    process_shapes(schema_graph, shapes)
                    
                    
    data_graph = Graph()
    data_graph.bind("cdh", CDH)

    annotation_graph = Graph()
    annotation_graph.bind("cdh", CDH)

    pts = {}
    psf = PairtreeStorageFactory()
    with gzip.open(args.csv_input, "rt") as ifd, zipfile.ZipFile(args.materials_output, "w") as zofd:
        c = csv.reader(ifd, delimiter="\t")        
        for i, toks in enumerate(c):
        # for i, (htid, access, rights, ht_bib_key, description, source,
        #         source_bib_num, oclc_num, isbn, issn, lccn, title,
        #         imprint, rights_reason_code, rights_timestamp,
        #         us_gov_doc_flag, rights_date_used, pub_place, lang,
        #         bib_fmt, collection_code, content_provider_code,
        #         responsible_entity_code, digitization_agent_code,
        #         access_profile_code, author) in enumerate(c):
            if len(data_graph) > 5000:
                partial(data_graph, args.data_output, author_ids, publisher_ids)
                schema_graph.serialize(destination=args.schema_output)
                annotation_graph.serialize(destination=args.annotation_output)
                break            
            access = toks[1]
            if access == "deny":
                pass
            else:
                try:
                    document_id = toks[0].strip()
                    doc_id_toks = document_id.split(".")
                    if args.hathitrust_path:
                        prefix = doc_id_toks[0]
                        rest = ".".join(doc_id_toks[1:])
                        pt = pts.setdefault(prefix, psf.get_store(store_dir=os.path.join(args.hathitrust_path, prefix)))
                        o = pt.get_object(rest, create_if_doesnt_exist=False)
                        part = o.list_parts()[0]
                        zf_name = [x for x in o.list_parts(part) if x.endswith("zip")]
                        zf = o.get_bytestream(os.path.join(part, zf_name[0]), streamable=True)
                        document_pages = []
                        with zipfile.ZipFile(zf, "r") as zifd:
                            for page in zifd.namelist():
                                document_pages.append(zifd.read(page).decode("utf-8"))                            
                        document_text = "\n".join(document_pages)
                    id_toks = document_id.split(".")
                    pairtree_name = id_toks[0].replace('/', '.')
                    pairtree_path = ".".join(id_toks[1:]).replace('/', '.')
                    enumeration = toks[4]
                    author = toks[25].strip()
                    title = toks[11].strip()
                    publisher_name = re.sub(r"[^a-zA-Z ]", "", re.sub(r"\d{4}", "", toks[12])).strip()
                    publication_year = toks[16].strip()
                    publication_date = "{}-01-01".format(publication_year) if publication_year else None
                    publication_place = toks[17].strip()
                    language = toks[18].strip()
                    document_type = toks[19].strip()
                    if int(publication_year) > 1800:
                        continue
                    for a, b, d in re.findall(r"([a-zA-Z][^\d]*)(?:(?P<birth>\d+)\-(?P<death>\d+)?)?", author):
                        m = re.match(r"^(?P<last>.*?)(?:,(?P<first>.*))?$", a)
                        if m:
                            last, first = m.groups()
                            last = ("" if not last else last.replace(",", "")).strip()
                            first = ("" if not first else first.replace(",", "")).strip()
                            b = int(b) if b else b
                            d = int(d) if d else d
                            author_key = (first, last, b, d)
                            if author_key not in author_ids:
                                hsh = md5()
                                hsh.update(str(author_key).encode())
                                author_ids[author_key] = hsh.hexdigest()
                            data_graph.add(
                                (
                                    CDH[document_id],
                                    SDO.creator,
                                    CDH[author_ids[author_key]]
                                )
                            )
                    data_graph.add(
                        (
                            CDH[document_id],
                            SDO.contentUrl,
                            URIRef("http://cdh.jhu.edu/materials/{}/{}".format(pairtree_name, pairtree_path))
                        )
                    )
                    if args.hathitrust_path:
                        arcname = os.path.join(pairtree_name, pairtree_path)
                        zofd.writestr(arcname, document_text)
                        zofd.writestr("{}.metadata".format(arcname), json.dumps({"content_type" : "text/plain"}))
                    data_graph.add(
                        (
                            CDH[document_id],
                            RDF.type,
                            SDO.CreativeWork
                        )
                    )
                    data_graph.add(
                        (
                            CDH[document_id],
                            SDO.name,
                            Literal(title)
                        )
                    )
                    data_graph.add(
                        (
                            CDH[document_id],
                            SDO.inLanguage,
                            Literal(language, datatype=XSD.language)
                        )
                    )
                    if enumeration:
                        data_graph.add(
                            (
                                CDH[document_id],
                                SDO.position,
                                Literal(enumeration)
                            )
                        )
                    if publication_date:
                        data_graph.add(
                            (
                                CDH[document_id],
                                SDO.datePublished,
                                Literal(publication_date, datatype=XSD.date)
                            )
                        )


                    publisher_key = (publisher_name, publication_place)
                    if publisher_name and publisher_key not in publisher_ids:
                        hsh = md5()
                        hsh.update(str(publisher_key).encode())
                        publisher_ids[publisher_key] = hsh.hexdigest()

                    if publisher_name:
                        data_graph.add(
                            (
                                CDH[document_id],
                                SDO.publisher,
                                CDH[publisher_ids[publisher_key]]
                            )
                        )
                except Exception as e:
                    print(e)
                    pass
                
