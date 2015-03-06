# -*- coding: utf-8 -*-

import list_duplicate_files
from fuzzy_substring import *
import csv
import os
import itertools

PATHS_TO_CHECK = ["images/", "failed_images/"]
FILE_FOR_DUPLICATES = "duplicates.csv"

MIN_SUBSTRING_SIMILARITY = 0.8
MAX_FUZZY_DISTANCE = 4

# Check for files that are exactly the same
def find_true_duplicates():
    print "Looking for true duplicates..."

    # Return list of lists of duplicates. Algorithm courtesy of 
    # http://code.activestate.com/recipes/551777/
    true_duplicates = list_duplicate_files.list_duplicates(PATHS_TO_CHECK)
    print "%d group found." % (len(true_duplicates))

    # For every group of duplicates, choose the one with best metadata and delete
    # the rest
    for group in true_duplicates:
        metadatas = read_metadatas(group)
        best_metadata = choose_best_metadata(metadatas)
        delete_files(group, except_for=best_metadata["file_name"])
        delete_metadata(group, except_for=best_metadata["file_name"])

# Given list of file names, find their corresponding metadata inside the CSVs
def read_metadatas(file_list):
    metadatas = []

    successful_obj = open("metadata.csv")
    successful_metadata = csv.DictReader(successful_obj)

    # For every row in metadata.csv
    for row in successful_metadata:
        file_name = row["file_name"]
        if file_name in file_list:
            metadatas.append(row)

    successful_obj.close()

    # If we found everything inside metadata.csv, no need to search further
    if len(metadatas) >= len(file_list):
        return metadatas

    rejected_obj = open("failed.csv")
    rejected_metadata = csv.DictReader(rejected_obj)

    # For every row in failed.csv
    for row in rejected_metadata:
        file_name = row["file_name"]
        if file_name in file_list:
            metadatas.append(row)

    rejected_obj.close()

    if len(metadatas) >= len(file_list):
        return metadatas
    else:
        raise Exception("Could not locate some duplicate files in CSV")
    
# From a list of metadatas, choose the one with the most fields filled out
def choose_best_metadata(metadatas):
    most_fields_present = 0
    best_index = -1

    for index, metadata in enumerate(metadatas):
        fields_present = len([True for v in metadata.values() if v != ""])

        # The 'problems' column doesn't count
        if "problems" in metadata:
            fields_present -= 1

        if fields_present >= most_fields_present:
            most_fields_present = fields_present
            best_index = index

    return metadatas[best_index]

def delete_files(file_list, except_for=None):
    for file_name in file_list:
        if file_name != except_for:
            os.remove(file_name)

def delete_metadata(file_list, except_for=None):
    file_list = [file_name for file_name in file_list if file_name != except_for]

    successful_obj = open("metadata.csv", "r+")
    lines = successful_obj.readlines()
    successful_obj.seek(0)

    for line in lines:
        # If there are lines left to delete...
        if len(file_list) > 0:
            line_is_okay = True
            
            # See if this line is subject to deletion
            for index, file_to_delete in enumerate(file_list):
                if file_to_delete + "," in line:
                    line_is_okay = False
                    del file_list[index]

            # If this line passed the test, write it in again.
            # If it didn't pass test, it'll be left out
            if line_is_okay:
                successful_obj.write(line)

    successful_obj.truncate()
    successful_obj.close()

    if len(file_list) == 0:
        return

    rejected_obj = open("failed.csv", "r+")
    lines = rejected_obj.readlines()
    rejected_obj.seek(0)

    for line in lines:
        if len(file_list) > 0:
            line_is_okay = True
            
            for index, file_to_delete in enumerate(file_list):
                if file_to_delete + "," in line:
                    line_is_okay = False
                    del file_list[index]
                    break

            if line_is_okay:
                rejected_obj.write(line)

    rejected_obj.truncate()
    rejected_obj.close()

    if len(file_list) > 0:
        raise Exception("Could not find all files in metadata for deletion")


def find_likely_duplicates():
    print
    print "Looking for likely duplicates..."
    suspects = scan_metadata_for_suspects()
    strong_suspects = scan_suspect_files(suspects)
    print "%d found." % (len(strong_suspects))
    arrest_suspects(strong_suspects)

def scan_metadata_for_suspects():
    suspects = []
    paintings = []

    successful_obj = open("metadata.csv")
    successful_metadata = csv.DictReader(successful_obj)
    for painting in successful_metadata:
        paintings.append(painting)
    successful_obj.close()

    rejected_obj = open("failed.csv")
    rejected_metadata = csv.DictReader(rejected_obj)
    for painting in rejected_metadata:
        paintings.append(painting)
    rejected_obj.close()

    # No longer necessary, artist names are normalized
    #paintings = [normalized(painting) for painting in paintings]

    paintings_by_artist = {}
    for painting in paintings:
        artist = painting["artist_normalized"]
        if artist in paintings_by_artist:
            paintings_by_artist[artist].append(painting)
        else:
            paintings_by_artist[artist] = [painting]

    for artist, associated_paintings in paintings_by_artist.iteritems():
        if len(associated_paintings) == 1:
            continue
        
        for pair in itertools.combinations(associated_paintings, 2):
            title_one, title_two = pair[0]["title"], pair[1]["title"]
            if len(title_one) == 0 or len(title_two) == 0:
                continue

            longest_common_substring = find_longest_common_substring(title_one, title_two)
            normalizer = min(len(title_one), len(title_two))

            similarity = len(longest_common_substring) / normalizer
            fuzzy_distance = fuzzy_levenshtein_distance(title_one, title_two)

            if similarity >= MIN_SUBSTRING_SIMILARITY or fuzzy_distance <= MAX_FUZZY_DISTANCE:
                suspects.append((pair[0], pair[1]))

    return suspects

        
# http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Longest_common_substring#Python2
def find_longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in xrange(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in xrange(1, 1 + len(s1)):
        for y in xrange(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]

# No longer necessary
def normalized(metadata):
    artist = metadata["artist"]
    artist = artist.lower()
    artist = artist.replace(" ", "")

    replacement_table = { ord(u"ä"): "a", ord(u"ü"): "u", ord(u"ö"): "o", ord(u"ß"): "ss", ord(u"é"): "e", ord(u"è"): "e", ord(u"à"): "a", ord(u"û"): "u", ord(u"ô"): "o"}
    for index, letter in enumerate(artist):
        if letter in replacement_table:
            artist[index] = replacement_table[letter]

    metadata["artist"] = artist
    return metadata

def scan_suspect_files(suspects):
    return suspects

def arrest_suspects(strong_suspects):
    duplicates_file = open(FILE_FOR_DUPLICATES, "a+")

    for gang in strong_suspects:
        file_paths = [suspect["file_name"] for suspect in gang]
        string_representation = ",".join(file_paths)
        duplicates_file.write(string_representation)

    duplicates_file.close()

def main():
    find_true_duplicates()
    find_likely_duplicates()

if __name__ == "__main__":
    main()
