def imc_avg (collection):
  return collection.aggregate([
    {
      "$unwind": "$types"
    },
    {
      "$match": {
        "$and": [
          { "size": { "$gt": 0 } },
          { "weight": { "$gt": 0 } }
        ]
      }
    },
    {
      "$group": {
        "_id": "$types",
        "avg": {
          "$avg": {
            "$divide": [
              "$weight",
              {
                "$multiply": ["$size", "$size"]
              }
            ]
          }
        }
      }
    },
    {
      "$sort": { "avg": -1 }
    }
  ])

def color_by_type (collection):
  return collection.aggregate([
    {
      "$unwind": "$types"
    },
    {
      "$group": {
        "_id": {
          "type": "$types",
          "color": "$color"
        },
        "count": {
          "$sum": 1
        }
      }
    },
    {
      "$sort": {
        "count": -1
      }
    },
    {
      "$group": {
        "_id": "$_id.type",
        "color": {
          "$first": "$_id.color"
        },
        "count": {
          "$first": "$count"
        }
      }
    }
  ])

def damage_taken_avg (collection):
  return collection.aggregate([
    {
      "$project": {
        "types": "$types",
        "damages": {
          "$objectToArray": "$damage_taken"
        }
      }
    },
    {
      "$match": {
        "damages": {
          "$gt": ["$size", 0]
        }
      }
    },
    {
      "$unwind": "$types"
    },
    {
      "$unwind": "$damages"
    },
    {
      "$group": {
        "_id": {
          "type": "$types",
          "damage_type": "$damages.k"
        },
        "damage_avg": {
          "$avg": "$damages.v"
        }
      }
    },
    {
      "$sort": {
        "damage_avg": -1
      }
    },
    {
      "$group": {
        "_id": "$_id.type",
        "damages_avg": {
          "$push": {
            "type": "$_id.damage_type",
            "avg": "$damage_avg"
          }
        }
      }
    }
  ])

def weight_double_filter (collection):
  return collection.aggregate([
    {
      "$unwind": "$evolutions"
    },
    {
      "$project": {
        "from": "$name",
        "to": "$evolutions.name",
        "from_weight": "$weight",
        "to_weight": "$evolutions.weight"
      }
    },
    {
      "$match": {
        "$and": [
          { "from_weight": { "$gt": 0 } },
          { "to_weight":   { "$gt": 0 } },
          {
            "$expr": {
              "$gt": ["$to_weight", { "$multiply": ["$from_weight", 2] }]
            }
          }
        ]
      }
    }
  ])

def type_change_filter (collection):
  return collection.aggregate([
    {
      "$unwind": "$evolutions"
    },
    {
      "$project": {
        "from": "$name",
        "to": "$evolutions.name",
        "from_types": "$types",
        "to_types": "$evolutions.types"
      }
    },
    {
      "$match": {
        "$and": [
          { "from_types.0": { "$exists": True }},
          { "to_types.0": { "$exists": True }},
          {
            "$expr": {
              "$ne": ["$from_types", "$to_types"]
            }
          }
        ]
      }
    }
  ])
