# Baltimore Dashboard

This folder is prepared for static hosting on GitHub Pages, Netlify, or Vercel.

## Publish on GitHub Pages

1. Create a new public GitHub repository, for example `baltimore-dashboard`.
2. Upload `index.html` from this folder to the root of the repo.
3. In GitHub, open `Settings` -> `Pages`.
4. Set `Source` to `Deploy from a branch`.
5. Select the `main` branch and the `/ (root)` folder.
6. Save and wait for the site to build.

Your site URL will look like:

`https://YOUR_GITHUB_USERNAME.github.io/baltimore-dashboard/`

## Notes

- The original export in `Downloads` was truncated at the end, so this copy includes a repaired map bootstrap.
- Share links now use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on live CDN and ArcGIS fetches at runtime.
