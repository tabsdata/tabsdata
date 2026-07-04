# API Credentials Template


```json
{
  "title": "<TITLE>",
  "category": "API_CREDENTIAL",
  "fields": [
    {
      "id": "<ID>",
      "label": "<LABEL>",
      "type": "STRING",
      "value": "<VALUE>"
    },
    {
      "id": "<ID>",
      "label": "<LABEL>",
      "type": "CONCEALED",
      "value": "<VALUE>"
    },
    {
      "id": "expires",
      "label": "expires",
      "type": "DATE",
      "value": "<YYYY-MM-DD>"
    }
 ]
}
```

## Delete

```bash
op item delete <ITEM> --vault=<VAULT>
```

## Create

If exists, first delete else you'll get duplicates.

```bash
op item create --vault=<VAULT> --template=<JSON_FILE>
```

## Get

```bash
op item get <ITEM> --vault=<VAULT> --format=json
```
