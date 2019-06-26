import sys
import json

def tag_converter(class_description_file):
    tag_dictionary = dict()
    class_descriptions = open(class_description_file, 'r')

    for line in class_descriptions:
        columns = line.replace("\n", ",").split(",")
        tag_dictionary[columns[0]] = columns[1]

    return tag_dictionary

def image_annotation_names(image_annotations_file,tag_dictionary):
    image_annotations_dictionary = dict()
    image_annotations = open(image_annotations_file, 'r')
    next(image_annotations)

    for line in image_annotations:
        columns = line.split(",")
        tag_name = tag_dictionary[columns[2]]
        if columns[0] in image_annotations_dictionary:
            image_annotations_dictionary[columns[0]].add(tag_name)
        else:
            image_annotations_dictionary[columns[0]] = {tag_name}

    for image_id, tags in image_annotations_dictionary.items():
        image_annotations_dictionary[image_id] = list(tags)
        print(image_annotations_dictionary[image_id])

    return image_annotations_dictionary

if __name__ == "__main__":
    args = sys.argv
    image_annotations_file = str(args[1])
    class_description_file = str(args[2])
    image_annotations_output_file = str(args[3])

    tag_dictionary = tag_converter(class_description_file)
    image_annotation_names = image_annotation_names(image_annotations_file, tag_dictionary)

    json.dump(image_annotation_names, open(image_annotations_output_file,'w'))
