# EvAtomic

This is an attempt to create a datomic like system in Postgresql.

Ultimately I'm not happy with the meta programming used in create_event_apply as it requires that it be called after all schema changes to a domain. Overall in increate in writes required by atomizing the events makes this approach questionable. Generic history tracking seems like a lighter weight approach for audit purposes if that's all you are after.
