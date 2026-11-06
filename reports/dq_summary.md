# Data quality summary (all days)

Full per-day report: `dq_report_by_day.csv` (and table `dq_report`).

| check | table | failed rows | days | action | example |
|---|---|---|---|---|---|
| rerender_collapsed | impressions | 429,393 | 62 | one fact row per fid (first render), render_count kept | `{"fid":"M2E0MWY0MjAzZmQ4YmE1YTow","impression_id":"25860fbfa7b0dfbf","created_at":"2026-09-06 22:33:56.604"}` |
| null_amount | orders | 306,182 | 62 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"order_id":"00003a85be9d29c4","site_id":"04a0b5b13c9aa179","placement_id":"a03105b8b0abc021","customer_id":"8401208474e7cf1c","created_at":"2026-09-25 22:13:33` |
| null_currency | orders | 277,711 | 62 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"order_id":"00003a85be9d29c4","site_id":"04a0b5b13c9aa179","placement_id":"a03105b8b0abc021","customer_id":"8401208474e7cf1c","created_at":"2026-09-25 22:13:33` |
| click_flood | clicks | 153,794 | 52 | kept; is_click counts once per slot; slot has > 100 clicks in total, review for fraud | `{"fid":"MmY0N2UxOTJlZWQ5YWMwNTow","_fid_clicks":"1002"}` |
| preview_traffic | fact_request | 74,639 | 29 | kept, flagged has_preview / is_preview in the ML mart so training can filter it | `{"request_id":"0001e72efeaf57c3","placement_id":"dcd4cd9e36f3e6ac"}` |
| late_arrival | impressions | 36,169 | 3 | dated by created_at; earlier fact partitions rebuilt | `{"impression_id":"0000c8c450fb9ad5","fid":"MzVkNzQyZmFjNjJhMDZlNDo0","placement_id":"b18bf18cdf04cce9","banner_id":"6780b09922349f92","created_at":"2026-10-03 0` |
| order_amount_without_currency | fact_request | 35,077 | 62 | no USD amount possible | `{"request_id":"00020e58fa88d3f5","placement_id":"f7e80e970636a990","order_amount_raw":"68.63"}` |
| late_arrival | requests | 24,041 | 1 | dated by created_at; earlier fact partitions rebuilt | `{"request_id":"000933362eff2ee2","placement_id":"f7e80e970636a990","created_at":"2026-10-03 13:18:32.347","session_id":"5837367f98afdd2d","user_agent":"Mozilla/` |
| no_dimension_match_banner | fact_impression | 24,029 | 62 | kept with banner_found=false | `{"fid":"M2E0MmQ1MmQ0YTlmNjA0MDow","request_id":"3a42d52d4a9f6040","placement_id":"f7e80e970636a990","banner_id":"10991be7724082be","impression_date":"2026-10-01` |
| late_arrival | orders | 14,067 | 1 | dated by created_at; earlier fact partitions rebuilt | `{"order_id":"00028ea0618f4257","site_id":"86acfa877e2fd318","placement_id":"3d2c6824fe8d1a8a","created_at":"2026-10-03 13:23:44","amount":"408.58","currency":"E` |
| placement_differs_from_request | fact_impression | 7,487 | 29 | kept; impression placement used for the placement dimension | `{"fid":"M2E1ZDFhMDI1YzgwMzFkYjow","request_id":"3a5d1a025c8031db","placement_id":"d3e49cdc6c0d3bc6","banner_id":"147f39cbdd24be72","impression_date":"2026-10-23` |
| no_dimension_match_placement | fact_request | 2,324 | 62 | kept with placement_found=false | `{"request_id":"00055b4e4807272d","placement_id":"c4c784225e3225a7"}` |
| country_without_holiday_calendar | fact_request | 1,973 | 62 | is_public_holiday = false in the ML mart (country not in the holidays package) | `{"request_id":"000a0e7a85e9eefe","country":"QA"}` |
| null_placement_id | requests | 1,959 | 62 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"_corrupt_record":"{\"request_id\":\"00489fa1a25147bd\",\"placement_id\":\"2f520d2d838c2bda\",\"created_at\":\"2026-09-04T05:13:18.475Z\",\"customer_id\":\"d64` |
| late_arrival | clicks | 1,664 | 1 | dated by created_at; earlier fact partitions rebuilt | `{"click_id":"00204bf297f398a9","fid":"MWI5MzUyMzBlMTkzNTdjYTow","created_at":"2026-10-03 15:18:34.151","status":"updated","load_date":"2026-10-04","_source_file` |
| orphan_no_impression | clicks | 869 | 62 | kept in staging only; cannot be attributed to a slot | `{"click_id":"0064d0c5d934fde1","fid":"NGM5ZDExNTA5NGI4YTAxNzox","created_at":"2026-09-10 03:43:00.545"}` |
| currency_without_fx_rate | fact_request | 761 | 59 | order_amount_usd = null in the ML mart (ECB/frankfurter has no rate for it) | `{"request_id":"0023b544444e3fcc","placement_id":"0d43889f1737be0f","order_amount_raw":"254.67","order_currency":"AED"}` |
| long_lag | postbacks | 640 | 21 | kept; conversion > 30 days after the impression, review attribution | `{"postback_id":"00592d52dc07f382","fid":"MmY0N2UxOTJlZWQ5YWMwNTow","first_render_ts":"2026-09-02 16:52:43.561","postback_ts":"2026-10-05 21:16:55.046","revenue"` |
| no_matching_request | fact_impression | 591 | 33 | kept with request_found=false; no request features in the ML mart | `{"fid":"M2E1NTQyZjY2MDExNDQyYjow","request_id":"3a5542f66011442b","placement_id":"d3e49cdc6c0d3bc6","banner_id":"147f39cbdd24be72","impression_date":"2026-10-12` |
| no_dimension_match_placement | fact_impression | 465 | 62 | kept with placement_found=false | `{"fid":"M2E5NDY2NWQzYTRhMWY4NTow","request_id":"3a94665d3a4a1f85","placement_id":"d6ba6c841111479b","banner_id":"3fddb4852d814ae7","impression_date":"2026-10-13` |
| null_banner_id | impressions | 269 | 25 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"impression_id":"00348da6b5f5e3e1","fid":"YWFjYzdiZTY4NDg1NmFlZToz","placement_id":"dcd4cd9e36f3e6ac","created_at":"2026-10-01 21:41:14.485","status":"updated"` |
| zero_revenue | postbacks | 146 | 19 | kept; counts as conversion, adds 0 revenue | `{"postback_id":"0110374598f30e7d","fid":"YWMyYWRlOTNiNDViYTkyMDox","revenue":"0.0"}` |
| late_arrival | postbacks | 104 | 31 | dated by created_at; earlier fact partitions rebuilt | `{"postback_id":"019015981c6c30ce","fid":"MDc5OWQxOWVhMTMzODZkMzoy","created_at":"2026-10-15 19:25:35.259","revenue":"19.46","postback_source":"Source 02","load_` |
| duplicate_id_in_snapshot | placement | 86 | 43 | kept the row with the latest updated_at | `{"id":"3d2c6824fe8d1a8a","load_date":"2026-09-12"}` |
| orphan_no_impression | postbacks | 60 | 36 | kept in fact_postback with impression_found=false (revenue not lost) | `{"postback_id":"00dc2c5a75f769af","fid":"OWMxYTNhMmM0YzRiYmUyZDow","created_at":"2026-10-25 23:35:06.49","revenue":"11.67"}` |
| duplicate_key | postbacks | 50 | 30 | kept first delivery; revenue counted once | `{"postback_id":"019015981c6c30ce","fid":"MDc5OWQxOWVhMTMzODZkMzoy","load_date":"2026-10-16","created_at":"2026-10-15 19:25:35.259","revenue":"19.46"}` |
| corrupt_record | requests | 20 | 2 | request_id / placement_id / created_at recovered by regex, flagged is_truncated_record | `{"load_date":"2026-09-04","_corrupt_record":"{\"request_id\":\"00489fa1a25147bd\",\"placement_id\":\"2f520d2d838c2bda\",\"created_at\":\"2026-09-04T05:13:18.475` |
| null_created_at | requests | 20 | 2 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"_corrupt_record":"{\"request_id\":\"00489fa1a25147bd\",\"placement_id\":\"2f520d2d838c2bda\",\"created_at\":\"2026-09-04T05:13:18.475Z\",\"customer_id\":\"d64` |
| null_request_id | requests | 20 | 2 | kept; downstream flag (e.g. placement_found=false, no USD amount) | `{"_corrupt_record":"{\"request_id\":\"00489fa1a25147bd\",\"placement_id\":\"2f520d2d838c2bda\",\"created_at\":\"2026-09-04T05:13:18.475Z\",\"customer_id\":\"d64` |
| duplicate_key | requests | 19 | 2 | kept first arrival only | `{"load_date":"2026-09-04"}` |

# Impression reconciliation per day

staging rows = fact rows + collapsed re-renders + rejected rows

| load_date | staging | fact | collapsed | rejected | difference | reconciled |
|---|---|---|---|---|---|---|
| 2026-08-24 | 22,637 | 18,976 | 3,661 | 0 | 0 | yes |
| 2026-08-25 | 25,372 | 22,124 | 3,248 | 0 | 0 | yes |
| 2026-08-26 | 25,536 | 22,558 | 2,978 | 0 | 0 | yes |
| 2026-08-27 | 25,453 | 22,402 | 3,051 | 0 | 0 | yes |
| 2026-08-28 | 33,994 | 26,983 | 7,011 | 0 | 0 | yes |
| 2026-08-29 | 35,399 | 25,418 | 9,981 | 0 | 0 | yes |
| 2026-08-30 | 33,797 | 24,504 | 9,293 | 0 | 0 | yes |
| 2026-08-31 | 30,716 | 26,364 | 4,352 | 0 | 0 | yes |
| 2026-09-01 | 28,614 | 28,191 | 423 | 0 | 0 | yes |
| 2026-09-02 | 37,425 | 28,040 | 9,385 | 0 | 0 | yes |
| 2026-09-03 | 32,825 | 26,287 | 6,538 | 0 | 0 | yes |
| 2026-09-04 | 31,898 | 24,484 | 7,414 | 0 | 0 | yes |
| 2026-09-05 | 28,535 | 23,742 | 4,793 | 0 | 0 | yes |
| 2026-09-06 | 28,710 | 24,035 | 4,675 | 0 | 0 | yes |
| 2026-09-07 | 28,307 | 23,999 | 4,308 | 0 | 0 | yes |
| 2026-09-08 | 31,055 | 27,218 | 3,837 | 0 | 0 | yes |
| 2026-09-09 | 29,541 | 25,959 | 3,582 | 0 | 0 | yes |
| 2026-09-10 | 31,509 | 28,130 | 3,379 | 0 | 0 | yes |
| 2026-09-11 | 31,869 | 28,440 | 3,429 | 0 | 0 | yes |
| 2026-09-12 | 28,693 | 24,417 | 4,276 | 0 | 0 | yes |
| 2026-09-13 | 27,686 | 23,539 | 4,147 | 0 | 0 | yes |
| 2026-09-14 | 33,863 | 28,254 | 5,609 | 0 | 0 | yes |
| 2026-09-15 | 35,548 | 29,850 | 5,698 | 0 | 0 | yes |
| 2026-09-16 | 34,282 | 29,628 | 4,654 | 0 | 0 | yes |
| 2026-09-17 | 43,800 | 38,964 | 4,836 | 0 | 0 | yes |
| 2026-09-18 | 47,929 | 42,930 | 4,999 | 0 | 0 | yes |
| 2026-09-19 | 47,478 | 41,591 | 5,887 | 0 | 0 | yes |
| 2026-09-20 | 57,754 | 50,571 | 7,183 | 0 | 0 | yes |
| 2026-09-21 | 52,739 | 47,100 | 5,639 | 0 | 0 | yes |
| 2026-09-22 | 51,763 | 46,114 | 5,649 | 0 | 0 | yes |
| 2026-09-23 | 52,442 | 46,619 | 5,823 | 0 | 0 | yes |
| 2026-09-24 | 54,463 | 47,446 | 7,017 | 0 | 0 | yes |
| 2026-09-25 | 44,189 | 38,298 | 5,891 | 0 | 0 | yes |
| 2026-09-26 | 38,324 | 33,471 | 4,853 | 0 | 0 | yes |
| 2026-09-27 | 38,190 | 31,944 | 6,246 | 0 | 0 | yes |
| 2026-09-28 | 39,142 | 33,127 | 6,015 | 0 | 0 | yes |
| 2026-09-29 | 44,148 | 37,687 | 6,461 | 0 | 0 | yes |
| 2026-09-30 | 39,935 | 33,623 | 6,312 | 0 | 0 | yes |
| 2026-10-01 | 34,628 | 27,951 | 6,677 | 0 | 0 | yes |
| 2026-10-02 | 30,352 | 25,532 | 4,820 | 0 | 0 | yes |
| 2026-10-03 | 0 | 0 | 0 | 0 | 0 | yes |
| 2026-10-04 | 62,315 | 51,310 | 11,005 | 0 | 0 | yes |
| 2026-10-05 | 35,671 | 29,431 | 6,240 | 0 | 0 | yes |
| 2026-10-06 | 36,265 | 30,654 | 5,611 | 0 | 0 | yes |
| 2026-10-07 | 34,071 | 28,487 | 5,584 | 0 | 0 | yes |
| 2026-10-08 | 32,052 | 25,716 | 6,336 | 0 | 0 | yes |
| 2026-10-09 | 35,013 | 28,278 | 6,735 | 0 | 0 | yes |
| 2026-10-10 | 38,084 | 30,438 | 7,646 | 0 | 0 | yes |
| 2026-10-11 | 38,692 | 30,928 | 7,764 | 0 | 0 | yes |
| 2026-10-12 | 38,800 | 31,564 | 7,236 | 0 | 0 | yes |
| 2026-10-13 | 57,811 | 42,201 | 15,610 | 0 | 0 | yes |
| 2026-10-14 | 51,262 | 37,520 | 13,742 | 0 | 0 | yes |
| 2026-10-15 | 51,047 | 38,021 | 13,026 | 0 | 0 | yes |
| 2026-10-16 | 47,505 | 36,567 | 10,938 | 0 | 0 | yes |
| 2026-10-17 | 46,077 | 35,218 | 10,859 | 0 | 0 | yes |
| 2026-10-18 | 50,299 | 38,069 | 12,230 | 0 | 0 | yes |
| 2026-10-19 | 56,790 | 43,864 | 12,926 | 0 | 0 | yes |
| 2026-10-20 | 63,685 | 48,544 | 15,141 | 0 | 0 | yes |
| 2026-10-21 | 60,539 | 47,155 | 13,384 | 0 | 0 | yes |
| 2026-10-22 | 48,513 | 42,177 | 6,336 | 0 | 0 | yes |
| 2026-10-23 | 72,210 | 60,653 | 11,557 | 0 | 0 | yes |
| 2026-10-24 | 76,466 | 67,020 | 9,446 | 0 | 0 | yes |
| 2026-10-25 | 68,904 | 62,893 | 6,011 | 0 | 0 | yes |
