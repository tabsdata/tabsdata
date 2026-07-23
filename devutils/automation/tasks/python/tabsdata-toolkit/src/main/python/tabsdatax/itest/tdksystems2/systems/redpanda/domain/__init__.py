#
# Copyright 2026 Tabsdata Inc.
#

# Redpanda domain resources. The domain verbs are the shared implementations in
# systems/_common/commands, except modify-data: Kafka is append-only, so it
# produces another batch of the model's rows as new messages to every topic
# rather than applying the shared insert/update/delete delta recipe.
