"""OpenObserve API module"""

# pylint: disable=too-many-arguments,bare-except,broad-exception-raised,broad-exception-caught,too-many-public-methods
import base64
import json

# import glob
import os
import sys
from pprint import pprint
from datetime import datetime
from collections.abc import MutableMapping
from typing import List, Dict, Union, cast
from pathlib import Path

import requests
import sqlglot  # type: ignore

try:
    import pandas

    HAVE_MODULE_PANDAS = True
except ImportError:
    print(
        "Can't import pandas. dataframe output, csv and xlsx export will be unavailable."
    )
    HAVE_MODULE_PANDAS = False


def flatten(dictionary, parent_key="", separator="."):
    """Flatten dictionary"""
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


class OpenObserve:
    """
    OpenObserve class based on OpenObserve REST API
    https://openobserve.ai/docs/api/
    https://<openobserve server>/swagger/
    """

    def __init__(
        self,
        user,
        password,
        *,
        organisation="default",
        host="http://localhost:5080",
        verify=True,
        timeout=10,
    ) -> None:
        bas64encoded_creds = base64.b64encode(
            bytes(user + ":" + password, "utf-8")
        ).decode("utf-8")
        self.openobserve_url = host + "/api/" + organisation + "/" + "[STREAM]"
        self.openobserve_host = host
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic " + bas64encoded_creds,
        }
        self.verify = verify
        self.timeout = timeout

    # pylint: disable=invalid-name
    def __timestampConvert(self, timestamp: datetime, verbosity: int = 0) -> int:
        try:
            if verbosity > 2:
                print(f"__timestampConvert input: {timestamp}")
            return int(timestamp.timestamp() * 1000000)
        except Exception as exc:
            # pylint: disable=raising-bad-type
            raise (  # type: ignore[misc]
                f"Exception from __timestampConvert for timestamp {timestamp}: {exc}"
            )

    # pylint: disable=invalid-name
    def __unixTimestampConvert(self, timestamp: int) -> datetime:
        try:
            timestamp_out = datetime.fromtimestamp(timestamp / 1000000)
        except:
            print("could not convert timestamp: " + str(timestamp))
        return timestamp_out

    def __intts2datetime(self, flatdict: dict) -> dict:
        for key, val in flatdict.items():
            if "time" in key:
                flatdict[key] = self.__unixTimestampConvert(val)
        return flatdict

    # pylint: disable=invalid-name
    def __datetime2Str(self, flatdict: dict) -> dict:
        for key, val in flatdict.items():
            if isinstance(val, datetime):
                flatdict[key] = self.__timestampConvert(val)
        return flatdict

    # pylint: disable=(missing-function-docstring
    def index(self, index: str, document: dict):
        assert isinstance(document, dict), "document must be a dict"
        # expects a flattened json
        document = flatten(document)
        document = self.__datetime2Str(document)

        res = requests.post(
            self.openobserve_url.replace("[STREAM]", index) + "/_json",
            headers=self.headers,
            json=[document],
            verify=self.verify,
            timeout=self.timeout,
        )
        if res.status_code != 200:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res_json = res.json()
        if res_json["status"][0]["failed"] > 0:
            raise Exception(
                f"Openobserve index failed. {res_json['status'][0]['error']}. document: {document}"
            )
        return res_json

    def search(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
    ) -> List[Dict]:
        """
        OpenObserve search function
        https://openobserve.ai/docs/api/search/search/
        """
        if isinstance(start_time, datetime):
            # convert to unixtime
            start_time = self.__timestampConvert(start_time, verbosity=verbosity)
        elif not isinstance(start_time, int):
            pprint("Error! start_time neither datetime, nor int")
            raise Exception("Search invalid start_time input")
        if isinstance(end_time, datetime):
            # convert to unixtime
            end_time = self.__timestampConvert(end_time, verbosity=verbosity)
        elif not isinstance(start_time, int):
            pprint("Error! end_time neither datetime, nor int")
            raise Exception("Search invalid end_time input")

        if verbosity > 1:
            pprint(f"Query Time start {start_time} end {end_time}")

        # verify sql
        try:
            sqlglot.transpile(sql)
        except sqlglot.errors.ParseError as e:
            raise e

        query = {"query": {"sql": sql, "start_time": start_time, "end_time": end_time}}
        if verbosity > 0:
            pprint(query)
        res = requests.post(
            self.openobserve_url.replace("/[STREAM]", "") + "/_search",
            json=query,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
        )
        if res.status_code != 200:
            raise Exception(
                f"Openobserve returned {res.status_code}. Text: {res.text}. url: {res.url}"
            )
        # timestamp back convert
        res_hits = res.json()["hits"]
        if verbosity > 3:
            pprint(res_hits)
        res_hits = [self.__intts2datetime(x) for x in res_hits]
        return res_hits

    def search2df(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
    ) -> pandas.DataFrame:
        """
        OpenObserve search function with df output
        https://openobserve.ai/docs/api/search/search/
        """
        # timestamp back convert

        # FIXME! set type for _timestamp column
        res_json_hits = self.search(
            sql, start_time=start_time, end_time=end_time, verbosity=verbosity
        )
        return pandas.json_normalize(res_json_hits)

    # pylint: disable=too-many-branches
    def export_objects_split(
        self,
        object_type: str,
        json_data: list[dict],
        file_path: str,
        *,
        verbosity: int = 0,
        flat: bool = False,
    ):
        """
        Export OpenObserve json configuration to split json files
        """
        key = "list"
        key2 = "name"
        if object_type == "dashboards":
            key = "dashboards"
            key2 = "dashboard_id"
        if object_type == "users":
            key = "data"
            key2 = "email"
        if verbosity > 3:
            pprint(json_data)
        if flat is True:
            dst_path = f"{file_path}{object_type}-"
        else:
            dst_path = f"{file_path}{object_type}/"
            Path(dst_path).mkdir(parents=True, exist_ok=True)
        if object_type in ("alerts/destinations", "alerts/templates"):
            if verbosity > 2:
                pprint("json_list set to alerts type")
            json_list = cast(List[Dict], json_data)
        else:
            try:
                json_list = cast(List[Dict], json_data[key])  # type: ignore[call-overload]
                if verbosity > 2:
                    pprint(f"json_list set to key {key}")
                    pprint(json_list)
            except:
                json_list = cast(List[Dict], [json_data])
                if verbosity > 2:
                    pprint("json_list set to array")
                    pprint(json_list)
        for json_object in json_list:
            if verbosity > 0:
                pprint(f"Export json {object_type} {json_object[key2]}...")
            if verbosity > 2:
                pprint(json_object)
            try:
                with open(
                    f"{dst_path}{json_object[key2]}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(json_object, f, ensure_ascii=False, indent=4)
            except Exception as err:
                pprint(f"Exception on json {object_type} {json_object[key2]}: {err}.")
        return True

    # pylint: disable=too-many-locals,too-many-statements
    def config_export(
        self,
        file_path: str,
        verbosity: int = 0,
        *,
        outformat: str = "json",
        split: bool = False,
        flat: bool = False,
    ):
        """
        Export OpenObserve configuration to json/csv/xlsx
        """
        if outformat == "json":
            # default json
            functions1 = self.list_objects("functions", verbosity=verbosity)
            pipelines1 = self.list_objects("pipelines", verbosity=verbosity)
            alerts1 = self.list_objects("alerts", verbosity=verbosity)
            alerts_destinations1 = self.list_objects(
                "alerts/destinations", verbosity=verbosity
            )
            alerts_templates1 = self.list_objects(
                "alerts/templates", verbosity=verbosity
            )
            dashboards1 = self.list_objects("dashboards", verbosity=verbosity)
            streams1 = self.list_objects("streams", verbosity=verbosity)
            users1 = self.list_objects("users", verbosity=verbosity)

            if split is True and flat is False:
                # split json
                self.export_objects_split(
                    "functions", functions1, file_path, verbosity=verbosity
                )
                self.export_objects_split(
                    "pipelines", pipelines1, file_path, verbosity=verbosity
                )
                self.export_objects_split(
                    "alerts", alerts1, file_path, verbosity=verbosity
                )
                self.export_objects_split(
                    "alerts/destinations",
                    alerts_destinations1,
                    file_path,
                    verbosity=verbosity,
                )
                self.export_objects_split(
                    "alerts/templates",
                    alerts_templates1,
                    file_path,
                    verbosity=verbosity,
                )
                self.export_objects_split(
                    "dashboards", dashboards1, file_path, verbosity=verbosity
                )
                self.export_objects_split(
                    "streams", streams1, file_path, verbosity=verbosity
                )
                self.export_objects_split(
                    "users", users1, file_path, verbosity=verbosity
                )
            elif split is True and flat is True:
                print("FIXME! Not implemented")
                sys.exit(1)
            else:
                # default json
                with open(f"{file_path}functions.json", "w", encoding="utf-8") as f:
                    json.dump(functions1, f, ensure_ascii=False, indent=4)
                with open(f"{file_path}pipelines.json", "w", encoding="utf-8") as f:
                    json.dump(pipelines1, f, ensure_ascii=False, indent=4)
                with open(f"{file_path}alerts.json", "w", encoding="utf-8") as f:
                    json.dump(alerts1, f, ensure_ascii=False, indent=4)
                with open(
                    f"{file_path}alerts-destinations.json", "w", encoding="utf-8"
                ) as f:
                    json.dump(alerts_destinations1, f, ensure_ascii=False, indent=4)
                with open(
                    f"{file_path}alerts-templates.json", "w", encoding="utf-8"
                ) as f:
                    json.dump(alerts_templates1, f, ensure_ascii=False, indent=4)
                with open(f"{file_path}dashboards.json", "w", encoding="utf-8") as f:
                    json.dump(dashboards1, f, ensure_ascii=False, indent=4)
                with open(f"{file_path}streams.json", "w", encoding="utf-8") as f:
                    json.dump(streams1, f, ensure_ascii=False, indent=4)
                with open(f"{file_path}users.json", "w", encoding="utf-8") as f:
                    json.dump(users1, f, ensure_ascii=False, indent=4)

        elif outformat in ("csv", "xlsx"):

            df_functions1 = self.list_objects2df("functions", verbosity=verbosity)
            df_pipelines1 = self.list_objects2df("pipelines", verbosity=verbosity)
            df_alerts1 = self.list_objects2df("alerts", verbosity=verbosity)
            df_alerts_destinations1 = self.list_objects2df(
                "alerts/destinations", verbosity=verbosity
            )
            df_alerts_templates1 = self.list_objects2df(
                "alerts/templates", verbosity=verbosity
            )
            df_dashboards1 = self.list_objects2df("dashboards", verbosity=verbosity)
            df_streams1 = self.list_objects2df("streams", verbosity=verbosity)
            df_users1 = self.list_objects2df("users", verbosity=verbosity)

            if outformat == "csv":
                df_functions1.to_csv(f"{file_path}functions.csv")
                df_pipelines1.to_csv(f"{file_path}pipelines.csv")
                df_alerts1.to_csv(f"{file_path}alerts.csv")
                df_alerts_destinations1.to_csv(f"{file_path}alerts-destinations.csv")
                df_alerts_templates1.to_csv(f"{file_path}alerts-templates.csv")
                df_dashboards1.to_csv(f"{file_path}dashboards.csv")
                df_streams1.to_csv(f"{file_path}streams.csv")
                df_users1.to_csv(f"{file_path}users.csv")
            elif outformat == "xlsx":
                df_functions1.to_excel(f"{file_path}functions.xlsx")
                df_pipelines1.to_excel(f"{file_path}pipelines.xlsx")
                df_alerts1.to_excel(f"{file_path}alerts.xlsx")
                df_alerts_destinations1.to_excel(f"{file_path}alerts-destinations.xlsx")
                df_alerts_templates1.to_excel(f"{file_path}alerts-templates.xlsx")
                df_dashboards1.to_excel(f"{file_path}dashboards.xlsx")
                df_streams1.to_excel(f"{file_path}streams.xlsx")
                df_users1.to_excel(f"{file_path}users.xlsx")

        pprint("Unknown outformat requested")

    def list_objects(self, object_type: str, verbosity: int = 0) -> List[Dict]:
        """
        List available objects for given type
        """
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}")
        res = requests.get(
            url, headers=self.headers, verify=self.verify, timeout=self.timeout
        )
        if verbosity > 0:
            pprint(url)
        if verbosity > 3:
            pprint(res)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res_json = res.json()
        return res_json

    def list_objects2df(self, object_type: str, verbosity: int = 0) -> pandas.DataFrame:
        """
        List available objects for given type
        Output: Dataframe
        """
        key: Union[str, int]
        key = "list"
        if object_type == "dashboards":
            key = "dashboards"
        if object_type == "users":
            key = "data"
        if object_type in ("alerts/destinations", "alerts/templates"):
            key = 0

        res_json = self.list_objects(object_type=object_type, verbosity=verbosity)

        return pandas.json_normalize(res_json[key])  # type: ignore[index]

    def create_object(
        self, object_type: str, object_json: dict, verbosity: int = 0
    ) -> bool:
        """
        Create object
        """
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}")
        if verbosity > 1:
            pprint(f"Create object {object_type} url: {url}")
        if verbosity > 2:
            pprint(f"Create object json input: {object_json}")
        res = requests.post(
            url,
            json=object_json,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
        )
        if verbosity > 1:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            pprint(f"Openobserve returned {res.status_code}. Text: {res.text}")
            return False
        if verbosity > 0:
            pprint("Create object completed")
        return True

    def update_object(
        self, object_type: str, object_json: dict, verbosity: int = 0
    ) -> bool:
        """
        Update object
        """
        url = self.openobserve_url.replace(
            "[STREAM]", f"{object_type}/{object_json['name']}"
        )
        if verbosity > 1:
            pprint(f"Update object {object_type} url: {url}")
        if verbosity > 2:
            pprint(f"Update object json input: {object_json}")
        res = requests.put(
            url,
            json=object_json,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
        )
        if verbosity > 3:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # pprint(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # return False
        if verbosity > 0:
            pprint("Update object completed")
        return True

    def import_objects_split(
        self,
        object_type: str,
        json_data: dict,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
    ) -> bool:
        """
        Import OpenObserve configuration from split json files
        """
        key2 = "name"
        if object_type == "dashboards":
            key2 = "dashboard_id"
        file = Path(file_path)
        if (json_data is None or not json_data) and file.exists():
            with open(file_path, "r", encoding="utf-8") as json_file:
                pprint(f"Load json data to import from file {file_path}")
                json_data = json.loads(json_file.read())
        elif json_data is None:
            pprint(
                "Fatal! import_objects_split(): input json_data None and file_path not exist"
            )
            return False
        if verbosity > 3:
            pprint(json_data)
        if verbosity > 0:
            pprint(f"Try to create {object_type} {json_data[key2]}...")
        try:
            res = self.create_object(object_type, json_data, verbosity=verbosity)
            pprint(f"Create returns {res}.")

            if res:
                return res

            if overwrite:
                print(f"Overwrite enabled. Updating object {json_data[key2]}")
                res = self.update_object(object_type, json_data, verbosity=verbosity)
                pprint(f"Update returns {res}.")
                return res

        except Exception as exc:
            raise Exception(f"Exception: {exc}") from exc
        return False

    def import_objects(
        self,
        object_type: str,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
        split: bool = False,
    ) -> bool:
        """
        Import objects from json
        Note: API does not import list of objects, need to do one by one.
        FIXME! dashboards are always imported as new creating duplicates. no idempotence.
        """
        if split is True:
            pprint(f"import_objects: search files in {file_path}")
            # functions
            # for file in glob.iglob(file_path + "functions/*.json"):
            for file in os.listdir(f"{file_path}"):
                if not file.endswith(".json"):
                    continue
                if verbosity > 1:
                    pprint(f"import_objects: file {file}")
                self.import_objects_split(
                    object_type,
                    {},
                    f"{file_path}/{file}",
                    overwrite=overwrite,
                    verbosity=verbosity,
                )
            return True

        key = "list"
        if object_type == "dashboards":
            key = "dashboards"
        if object_type == "users":
            key = "data"
        with open(file_path, "r", encoding="utf-8") as json_file:
            json_data = json.loads(json_file.read())
            if verbosity > 3:
                pprint(json_data)
            if object_type in ("alerts/destinations", "alerts/templates"):
                json_list = json_data
            else:
                try:
                    json_list = json_data[key]
                except:
                    json_list = [json_data]
            for json_object in json_list:
                self.import_objects_split(
                    object_type,
                    json_object,
                    "",
                    overwrite=overwrite,
                    verbosity=verbosity,
                )
        return True

    def config_import(
        self,
        object_type: str,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
        split: bool = False,
    ):
        """
        Import OpenObserve configuration from json
        object_type all does everything
        """
        if object_type == "all" and split is True:
            self.import_objects(
                "functions",
                f"{file_path}functions",
                overwrite=overwrite,
                verbosity=verbosity,
                split=split,
            )
            self.import_objects(
                "pipelines",
                f"{file_path}pipelines",
                overwrite=overwrite,
                verbosity=verbosity,
                split=split,
            )
            # FIXME! Return 404. Text:
            # self.import_objects(
            #     "alerts", f"{file_path}alerts", overwrite=overwrite, verbosity=verbosity, split
            # )
            # FIXME! ('Return 400. Text: {"code":400,
            #     "message":"Email destination must have SMTP ' 'configured"}')
            # self.import_objects(
            #     "alerts/destinations",
            #     f"{file_path}alerts/destinations",
            #     overwrite=overwrite,
            #     verbosity=verbosity,
            #     split=split,
            # )
            self.import_objects(
                "alerts/templates",
                f"{file_path}alerts/templates",
                overwrite=overwrite,
                verbosity=verbosity,
                split=split,
            )
            self.import_objects(
                "dashboards",
                f"{file_path}dashboards",
                overwrite=overwrite,
                verbosity=verbosity,
                split=split,
            )
        elif object_type == "all":
            self.import_objects(
                "functions",
                f"{file_path}functions.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            self.import_objects(
                "pipelines",
                f"{file_path}pipelines.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            self.import_objects(
                "alerts",
                f"{file_path}alerts.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            self.import_objects(
                "alerts/destinations",
                f"{file_path}alerts-destinations.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            self.import_objects(
                "alerts/templates",
                f"{file_path}alerts-templates.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            self.import_objects(
                "dashboards",
                f"{file_path}dashboards.json",
                overwrite=overwrite,
                verbosity=verbosity,
            )
            # No CreateStream, only CreateStreamSettings
            # self.import_objects('streams', f"{file_path}streams.json", overwrite, verbosity)
            # "Return 400. Text:
            #     Json deserialize error: missing field `password` at line 1" = Extra field required
            # self.import_objects('users', f"{file_path}users.json", overwrite, verbosity)
        else:
            self.import_objects(
                object_type, file_path, overwrite=overwrite, verbosity=verbosity
            )
