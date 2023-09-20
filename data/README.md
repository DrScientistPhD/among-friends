# Data Directory

*The mocked_data directory contains only the minimum files required to locally generate the Among Friends dashboard, 
and does not contain all the files or columns listed below.*

This directory contains several CSV files related to the project. As no formal documentation is provided by Signal, the following is inferred by the repository author and may contain inaccurate information.:

## group_memberships.csv

This CSV file contains data about group memberships.

1. **_id (integer)**: The unique identifier for each row.
2. **group_id (string)**: This column contains unique identifiers for groups. 
<br>Example: "signal_group__v2!543f70f9ce63f8f8d03480e3d..."
3. **recipient_id (integer)**: This column contains identifiers for recipients.

The file represents a many-to-many relationship between groups and recipients. Each row may represent a recipient's membership in a group.

## groups.csv

This CSV file contains data about groups.

1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **group_id (string)**: This column contains unique identifiers for groups.
3. **recipient_id (integer)**: This column contains identifiers for recipients.
4. **title (string)**: This column contains the titles of the groups.
5. **avatar_id (integer)**: An identifier related to the group's avatar.
6. **avatar_key (float)**: This column is always empty in the provided data.
7. **avatar_content_type (float)**: This column is always empty in the provided data.
8. **avatar_relay (float)**: This column is always empty in the provided data.
9. **timestamp (integer)**: This could be the timestamp related to the creation or last modification of the group.
10. **active (integer)**: This might indicate whether the group is active or not.
11. **avatar_digest (float)**: This column is always empty in the provided data.
12. **mms (integer)**: The purpose of this column is unclear.
13. **master_key (string)**: This appears to be some kind of key associated with the group.
14. **revision (integer)**: This might be a revision number for the group data.
15. **decrypted_group (string)**: This appears to be some kind of decrypted information about the group.
16. **expected_v2_id (float)**: This column is always empty in the provided data.
17. **former_v1_members (float)**: This column is always empty in the provided data.
18. **distribution_id (string)**: This appears to be an identifier for some kind of distribution related to the group.
19. **display_as_story (integer)**: This might indicate whether the group should be displayed as a story or not.
20. **auth_service_id (float)**: This column is always empty in the provided data.
21. **last_force_update_timestamp (integer)**: This could be the timestamp of the last forced update on the group.

## mention.csv

This CSV file contains data about mentions.

1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **thread_id (integer)**: This column contains identifiers for threads.
3. **message_id (integer)**: This column contains identifiers for messages.
4. **recipient_id (integer)**: This column contains identifiers for recipients.
5. **range_start (integer)**: This column might represent the starting point of a range, possibly indicating where in a message a mention starts.
6. **range_length (integer)**: This column might represent the length of a range, possibly indicating the length of a mention within a message.

## message.csv

This CSV file contains data about messages.

1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **date_sent (integer)**: This column might represent the timestamp when the message was sent.
3. **date_received (integer)**: This column might represent the timestamp when the message was received.
4. **date_server (integer)**: This column might represent the timestamp from the server related to the message.
5. **thread_id (integer)**: This column contains identifiers for threads.
6. **from_recipient_id (integer)**: This column contains identifiers for the sender of the message.
7. **from_device_id (float)**: This column might contain identifiers for the device from which the message was sent.
8. **to_recipient_id (integer)**: This column contains identifiers for the recipient of the message.
9. **type (integer)**: This column might represent the type of the message.
10. **body (string)**: This column contains the body of the message.
11. **read (integer)**: This column might represent whether the message has been read.
12. **ct_l (float)**: This column is always empty in the provided data.
13. **exp (float)**: The purpose of this column is unclear.
14. **m_type (float)**: This might represent some type associated with the message.
15. **m_size (float)**: This might represent the size of the message.
16. **st (float)**: The purpose of this column is unclear.
17. **tr_id (float)**: This column is always empty in the provided data.
18. **subscription_id (integer)**: This might represent a subscription associated with the message.
19. **receipt_timestamp (integer)**: This might represent the timestamp of the receipt of the message.
20. **delivery_receipt_count (integer)**: This might represent the count of delivery receipts for the message.
21. **read_receipt_count (integer)**: This might represent the count of read receipts for the message.
22. **viewed_receipt_count (integer)**: This might represent the count of viewed receipts for the message.
23. **mismatched_identities (float)**: This column is always empty in the provided data.
24. **network_failures (float)**: This column is always empty in the provided data.
25. **expires_in (integer)**: This might represent the time in which the message expires.
26. **expire_started (integer)**: This might represent the time when the expiration of the message started.
27. **notified (integer)**: This might represent whether a notification was sent for the message.
28. **quote_id (float)**: This might represent an id associated with a quote in the message. NOTE: It matches the timestamp of the message being quoted.
29. **quote_author (float)**: This might represent the author of a quote in the message.
30. **quote_body (string)**: This might represent the body of a quote in the message.
31. **quote_missing (integer)**: This might represent whether a quote in the message is missing.
32. **quote_mentions (float)**: This column is always empty in the provided data.
33. **quote_type (float)**: This might represent the type of a quote in the message.
34. **shared_contacts (float)**: This column is always empty in the provided data.
35. **unidentified (integer)**: This might represent whether the message is unidentified.
36. **link_previews (string)**: This might contain a preview of links in the message.
37. **view_once (integer)**: This might represent whether the message can only be viewed once.
38. **reactions_unread (integer)**: This might represent the count of unread reactions to the message.
39. **reactions_last_seen (integer)**: This might represent the timestamp of the last seen reaction to the message.
40. **remote_deleted (integer)**: This might represent whether the message was deleted remotely.
41. **mentions_self (integer)**: This might represent whether the message mentions self.
42. **notified_timestamp (integer)**: This might represent the timestamp of a notification for the message.
43. **server_guid (string)**: This might represent a unique identifier from the server for the message.
44. **message_ranges (string)**: This might represent ranges in the message.
45. **story_type (integer)**: This might represent the type of a story in the message.
46. **parent_story_id (integer)**: This might represent the id of a parent story for the story in the message.
47. **export_state (float)**: This column is always empty in the provided data.
48. **exported (integer)**: This might represent whether the message was exported.
49. **scheduled_date (integer)**: This might represent a scheduled date for the message.
50. **latest_revision_id (float)**: This column is always empty in the provided data.
51. **original_message_id (float)**: This column is always empty in the provided data.
52. **revision_number (integer)**: This might represent a revision number for the message.

## reactions.csv

This CSV file contains data about reactions to messages.

1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **message_id (integer)**: This column contains identifiers for messages, likely indicating which message the reaction is associated with.
3. **author_id (integer)**: This column contains identifiers for the author of the reaction.
4. **emoji (string)**: This column contains emojis, which are likely the reactions themselves.
5. **date_sent (integer)**: This column might represent the timestamp when the reaction was sent.
6. **date_received (integer)**: This column might represent the timestamp when the reaction was received.

## recipient.csv

This CSV file contains data about recipients.

1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **uuid (string)**: This column might contain a universally unique identifier for the recipient.
3. **username (float)**: This column is always empty in the provided data.
4. **phone (float)**: This column might contain the phone number of the recipient.
5. **email (float)**: This column is always empty in the provided data.
6. **group_id (string)**: This column might contain identifiers for groups that the recipient is a part of.
7. **group_type (integer)**: This column might represent the type of the group that the recipient is a part of.
8. **blocked (integer)**: This column might indicate whether the recipient is blocked.
9. **message_ringtone (float)**: This column is always empty in the provided data.
10. **message_vibrate (integer)**: This column might indicate whether the recipient's device vibrates upon receiving a message.

## remapped_recipients.csv

This CSV file contains data about remapped recipients. The data in this file might be used to update or migrate recipient identifiers from an old system or format to a new one.


1. **_id (integer)**: This seems to be a unique identifier for each row.
2. **old_id (integer)**: This column contains the old identifiers for the recipients.
3. **new_id (integer)**: This column contains the new identifiers for the recipients.

<br>Please refer to the individual files for a more detailed look at the data.
