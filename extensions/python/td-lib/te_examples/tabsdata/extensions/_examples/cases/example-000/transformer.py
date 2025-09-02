import tabsdata as td


@td.transformer(input_tables="persons", output_tables=["spanish", "french", "german"])
def tfr(persons: td.TableFrame) -> (td.TableFrame, td.TableFrame, td.TableFrame):
    persons = persons.select(
        ["identifier", "name", "surname", "nationality", "language"]
    )
    res = {}
    for nationality in ["Spanish", "French", "German"]:
        res[nationality] = persons.filter(td.col("nationality").eq(nationality)).drop(
            "nationality"
        )
    return res["Spanish"], res["French"], res["German"]
