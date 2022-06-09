/*
 Author Mimoune Djouallah
 copied from this blog https://blog.crossjoin.co.uk/2021/03/07/parquet-files-in-power-bi-power-query-and-the-streamed-binary-values-error/
*/



let
  parquetfunctin = (params) =>

    let
       
       Parquet = Parquet.Document(Binary.Buffer(Web.Contents("https://nyc-tlc.s3.amazonaws.com/trip+data/",[RelativePath=params])))
  
    in
        Parquet,

location=
   let
      Source = Csv.Document(Web.Contents("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
       #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
       #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}}, "en-AU")
    in
        #"Changed column type",
        
  Source = Table.FromColumns({Lines.FromBinary(Web.Contents("https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"))}),
  #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Column1], "<li><a href=""https://s3.amazonaws.com/nyc-tlc/trip+data/")),
  #"Split Column by Delimiter" = Table.SplitColumn(#"Filtered Rows", "Column1", Splitter.SplitTextByDelimiter("title=""", QuoteStyle.None), {"Column1.1", "Column1.2"}),
  #"Removed Other Columns" = Table.SelectColumns(#"Split Column by Delimiter", {"Column1.1"}),
  #"Replaced Value" = Table.ReplaceValue(#"Removed Other Columns", "<li><a href=""https://s3.amazonaws.com/nyc-tlc/trip+data/", "", Replacer.ReplaceText, {"Column1.1"}),
  #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value", """", "", Replacer.ReplaceText, {"Column1.1"}),
  #"Renamed Columns" = Table.RenameColumns(#"Replaced Value1", {{"Column1.1", "URL"}}),
  #"Trimmed Text" = Table.TransformColumns(#"Renamed Columns",{{"URL", Text.Trim, type text}}),
  #"Cleaned Text" = Table.TransformColumns(#"Trimmed Text",{{"URL", Text.Clean, type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Cleaned Text", each Text.StartsWith([URL], "yellow")),
                                                                                                     /* Filter the number of files to download */
  #"Filter the number of files" = Table.FirstN(#"Filtered rows 1", 2),
  #"Added Custom" = Table.AddColumn(#"Filter the number of files", "Custom", each parquetfunctin([URL])),
  #"Expanded Custom" = Table.ExpandTableColumn(#"Added Custom", "Custom", {"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"}, {"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Custom", {{"tpep_pickup_datetime", type datetime}, {"tpep_dropoff_datetime", type datetime}, {"VendorID", Int64.Type}, {"store_and_fwd_flag", type text}, {"passenger_count", Int64.Type}, {"PULocationID", Int64.Type}, {"DOLocationID", Int64.Type}, {"payment_type", Int64.Type}, {"RatecodeID", Int64.Type}, {"trip_distance", type number}, {"congestion_surcharge", type number}, {"airport_fee", type number}, {"total_amount", type number}, {"improvement_surcharge", type number}, {"tolls_amount", type number}, {"tip_amount", type number}, {"mta_tax", type number}, {"extra", type number}, {"fare_amount", type number}}),
  #"Inserted time" = Table.AddColumn(#"Changed column type", "Pickup_Time", each DateTime.Time([tpep_pickup_datetime]), type nullable time),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted time", {{"tpep_pickup_datetime", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"tpep_pickup_datetime", "tpep_pickup"}}),
  #"Inserted time 1" = Table.AddColumn(#"Renamed columns 1", "tpep_dropoff_time", each DateTime.Time([tpep_dropoff_datetime]), type nullable time),
  #"Calculated date" = Table.TransformColumns(#"Inserted time 1", {{"tpep_dropoff_datetime", each DateTime.Date(_), type nullable date}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Calculated date", {{"tpep_dropoff_datetime", "tpep_dropoff_date"}}),
  #"Inserted year" = Table.AddColumn(#"Renamed columns 2", "tpep_pickup_year", each Date.Year([tpep_pickup]), type nullable number),
  #"Merged queries" = Table.NestedJoin(#"Inserted year", {"PULocationID"}, location, {"LocationID"}, "location", JoinKind.LeftOuter),
  #"Expanded location" = Table.ExpandTableColumn(#"Merged queries", "location", {"Borough", "Zone", "service_zone"}, {"Borough", "Zone", "service_zone"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Expanded location", {{"Borough", "Pickup_Borough"}, {"Zone", "Pickup_Zone"}, {"service_zone", "Pickup_service_zone"}}),
  #"Merged queries 1" = Table.NestedJoin(#"Renamed columns 3", {"DOLocationID"}, location, {"LocationID"}, "location", JoinKind.LeftOuter),
  #"Expanded location 1" = Table.ExpandTableColumn(#"Merged queries 1", "location", {"Borough", "Zone", "service_zone"}, {"Borough", "Zone", "service_zone"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Expanded location 1", {{"Borough", "Dropoff_Borough"}, {"Zone", "Dropoff_Zone"}, {"service_zone", "Dropoff_service_zone"}})
in
  #"Renamed columns 4"
