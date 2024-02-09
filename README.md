## Select samples

Use select_sample_projects.py to extract the samples. 

If you want to use the Public Jira Dataset from Montgomery et al. (2022), make sure you first process your data as in convert_oss_json.py.
Run after this:
```
python select_sample_projects.py 0
```
Perform the following to select a sample from the TAWOS dataset of Tawosi et al. (2022):

```
python select_sample_projects.py 1
```

## Extract results
To create the results, first run merge_nvivo_datasets. 
This file creates an excel file that indicates by item which categories occur (0 or 1 only).

This file is used by link_codes_to_df.py to also indicate per item whether a category occurs more than once.

Finally, the notebook presents all the results


## Sources
Montgomery, L., Lüders, C., & Maalej, W. (2022, May). An alternative issue tracking dataset of public jira repositories. In Proceedings of the 19th International Conference on Mining Software Repositories (pp. 73-77).

Tawosi, V., Al-Subaihin, A., Moussa, R., & Sarro, F. (2022, May). A versatile dataset of agile open source software projects. In Proceedings of the 19th International Conference on Mining Software Repositories (pp. 707-711).
