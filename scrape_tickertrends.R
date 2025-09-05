#!/usr/bin/env Rscript

# ────────────────────────────────────────────────────────────────────────────
# scrape_tickertrends.R
#   • Scrape TickerTrends sections and upsert to Supabase (Postgres)
#   • Sections → tables:
#       - /t/whats-trending     → whats_trending
#       - /t/stock-market       → deep_dives
#       - /t/earnings-preview   → earnings_preview
# ────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(rvest)
  library(xml2)
  library(httr)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(lubridate)
  library(DBI)
  library(RPostgres)
})

# ── Env helpers ─────────────────────────────────────────────────────────────
trim_env <- function(x, default = "") {
  v <- Sys.getenv(x, unset = default)
  v <- str_trim(v)
  if (identical(v, "")) default else v
}
num_env <- function(x, default) {
  v <- suppressWarnings(as.numeric(trim_env(x, as.character(default))))
  ifelse(is.na(v), default, v)
}

BASE_URL            <- trim_env("TT_BASE_URL", "https://blog.tickertrends.io")
MAX_ARTICLES        <- num_env("MAX_ARTICLES_PER_TAG", 40)   # per section
HTTP_TIMEOUT_SEC    <- num_env("SCRAPE_TIMEOUT_SEC", 20)
USER_AGENT_STR      <- trim_env("SCRAPE_UA",
                                "Mozilla/5.0 (compatible; TT-Scraper/1.0; +https://blog.tickertrends.io)"
)

PG_HOST <- trim_env("SUPABASE_HOST")
PG_PORT <- as.integer(trim_env("SUPABASE_PORT", "5432"))
PG_DB   <- trim_env("SUPABASE_DB", "postgres")
PG_USER <- trim_env("SUPABASE_USER")
PG_PWD  <- trim_env("SUPABASE_PWD")

stopifnot(PG_HOST != "", PG_USER != "", PG_PWD != "")

# ── HTTP helpers ────────────────────────────────────────────────────────────
ua <- httr::user_agent(USER_AGENT_STR)

fetch_html <- function(url, tries = 3, timeout_sec = HTTP_TIMEOUT_SEC) {
  for (i in seq_len(tries)) {
    resp <- tryCatch(
      httr::GET(url, ua, httr::timeout(timeout_sec)),
      error = identity
    )
    if (inherits(resp, "error")) {
      if (i == tries) stop("HTTP error for ", url, ": ", resp$message)
    } else if (httr::http_error(resp)) {
      if (i == tries) stop("Non-200 for ", url, ": ", httr::status_code(resp))
    } else {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      return(xml2::read_html(txt))
    }
    Sys.sleep(1.5 * i)
  }
  stop("Failed to fetch: ", url)
}

norm_url <- function(href, base = BASE_URL) {
  href <- str_trim(href %||% "")
  if (href == "") return(NA_character_)
  if (grepl("^https?://", href, ignore.case = TRUE)) return(href)
  paste0(gsub("/+$", "", base), "/", gsub("^/+", "", href))
}

`%||%` <- function(a, b) if (is.null(a)) b else a

# ── Parsing helpers ─────────────────────────────────────────────────────────
# Parse "Aug 20, 2025" | "Aug 20" | "23 hrs ago" | "yesterday" | "today"
parse_issue_date <- function(x, tz = Sys.timezone()) {
  now_ts <- lubridate::now(tzone = tz)
  # 1) Full dates
  d <- suppressWarnings(mdy(x, quiet = TRUE))
  # 2) Add current year if missing
  miss <- is.na(d)
  if (any(miss)) {
    d2 <- suppressWarnings(mdy(paste0(x[miss], ", ", year(now_ts)), quiet = TRUE))
    d[miss] <- d2
  }
  # 3) Relative phrases
  still <- is.na(d)
  if (any(still)) {
    rel <- tolower(trimws(x[still]))
    out <- rep(as.Date(NA), length(rel))
    
    pull_num <- function(pat, conv) {
      m <- stringr::str_match(rel, pat)
      hit <- !is.na(m[, 1])
      if (any(hit)) out[hit] <<- as.Date(conv(as.numeric(m[hit, 2])))
    }
    pull_num("^(\\d+)\\s*(hr|hrs|hour|hours)\\s*ago$", \(n) now_ts - hours(n))
    pull_num("^(\\d+)\\s*(min|mins|minute|minutes)\\s*ago$", \(n) now_ts - minutes(n))
    pull_num("^(\\d+)\\s*(day|days)\\s*ago$", \(n) now_ts - days(n))
    
    out[str_detect(rel, "^yesterday$")] <- as.Date(now_ts - days(1))
    out[str_detect(rel, "^today$")]     <- as.Date(now_ts)
    
    d[still] <- out
  }
  as.Date(d)
}

slugify <- function(x) {
  x %>%
    iconv(from = "UTF-8", to = "ASCII//TRANSLIT") %>%
    tolower() %>%
    gsub("[^a-z0-9]+", "-", .) %>%
    gsub("(^-+|-+$)", "", .)
}

# Extract article body text (keep it simple + robust)
extract_article_text <- function(url) {
  doc <- fetch_html(url)
  txt <- doc %>%
    html_elements("strong, .header-anchor-post span, .markup p, article p, article h2, article h3") %>%
    html_text2() %>%
    paste(collapse = "\n\n") %>%
    str_squish()
  if (!nzchar(txt)) {
    # fallback: grab all <p>
    txt <- doc %>% html_elements("p") %>% html_text2() %>% paste(collapse = "\n\n") %>% str_squish()
  }
  txt %||% ""
}

# Scrape one index page (section)
scrape_index <- function(tag_slug) {
  idx_url <- paste0(gsub("/+$", "", BASE_URL), "/t/", tag_slug)
  doc <- fetch_html(idx_url)
  
  # These selectors mirror your working code; keep them but add fallbacks
  title_nodes <- html_elements(doc, ".clamp-3-lxFDfR")
  date_nodes  <- html_elements(doc, ".date-rtYe1v")
  
  titles <- html_text2(title_nodes)
  hrefs  <- html_attr(title_nodes, "href")
  dates  <- html_text2(date_nodes)
  
  # Fallback if counts mismatch or nothing found
  if (length(titles) == 0 || length(hrefs) == 0) {
    # Try more generic cards: links inside article cards
    links <- html_elements(doc, "a[href*='/p/'], a[href*='/t/']")  # generic
    titles <- html_text2(links)
    hrefs  <- html_attr(links, "href")
    # try to find nearby date nodes
    dates  <- dates %||% character(length(titles))
  }
  
  n <- min(length(titles), length(hrefs), length(dates))
  if (n == 0) return(tibble(name = character(), date = character(), url = character()))
  
  tibble(
    name = titles[seq_len(n)],
    date = dates[seq_len(n)],
    url  = vapply(hrefs[seq_len(n)], norm_url, character(1))
  ) %>%
    filter(nzchar(name), nzchar(url)) %>%
    slice_head(n = MAX_ARTICLES)
}

# End-to-end: scrape a section and build the normalized frame for DB
build_section_df <- function(tag_slug) {
  idx <- scrape_index(tag_slug)
  if (nrow(idx) == 0) return(tibble())
  
  # Fetch article bodies (with small politeness delay)
  contents <- map_chr(idx$url, function(u) {
    out <- tryCatch(extract_article_text(u), error = function(e) "")
    Sys.sleep(0.3)  # be polite
    out
  })
  
  raw <- mutate(idx, cast = contents)  # keep your original field name 'cast'
  
  wt <- raw %>%
    transmute(
      issue_date = parse_issue_date(date),
      name       = as.character(name),
      content    = as.character(cast),
      wt_key     = paste0(format(issue_date, "%Y%m%d"), "_", slugify(name))
    ) %>%
    filter(!is.na(issue_date), nzchar(wt_key)) %>%
    distinct(wt_key, .keep_all = TRUE)
  wt
}

# ── DB helpers ──────────────────────────────────────────────────────────────
get_con <- function() {
  dbConnect(
    RPostgres::Postgres(),
    host     = PG_HOST,
    port     = PG_PORT,
    dbname   = PG_DB,
    user     = PG_USER,
    password = PG_PWD,
    sslmode  = "require"
  )
}

ensure_table <- function(con, table_name) {
  sql <- sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      wt_key      text PRIMARY KEY,
      issue_date  date,
      name        text,
      content     text,
      created_at  timestamptz DEFAULT now()
    );
  ", DBI::dbQuoteIdentifier(con, table_name))
  invisible(dbExecute(con, sql))
}

upsert_wt <- function(con, table_name, wt_df) {
  if (!nrow(wt_df)) return(invisible(0L))
  tmp_name <- paste0("tmp_", table_name)
  dbWriteTable(con, tmp_name, wt_df, temporary = TRUE, overwrite = TRUE)
  
  sql <- sprintf("
    INSERT INTO %s AS t (wt_key, issue_date, name, content)
    SELECT wt_key, issue_date, name, content
    FROM %s
    ON CONFLICT (wt_key) DO UPDATE SET
      issue_date = EXCLUDED.issue_date,
      name       = EXCLUDED.name,
      content    = EXCLUDED.content;
  ",
                 DBI::dbQuoteIdentifier(con, table_name),
                 DBI::dbQuoteIdentifier(con, tmp_name)
  )
  n <- dbExecute(con, sql)
  dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", DBI::dbQuoteIdentifier(con, tmp_name)))
  invisible(n)
}

# ── Run all sections ────────────────────────────────────────────────────────
sections <- list(
  list(tag = "whats-trending",   table = "whats_trending"),
  list(tag = "stock-market",     table = "deep_dives"),
  list(tag = "earnings-preview", table = "earnings_preview")
)

con <- get_con()
on.exit(try(dbDisconnect(con), silent = TRUE))

dbGetQuery(con, "select '✅ connected' as status, now();")

for (s in sections) {
  cat(sprintf("\n—— Scraping /t/%s → %s ——\n", s$tag, s$table))
  wt <- build_section_df(s$tag)
  cat(sprintf("Found %d rows after normalization\n", nrow(wt)))
  
  ensure_table(con, s$table)
  upsert_wt(con, s$table, wt)
  
  cat(sprintf("✅ Upserted into %s\n", s$table))
}

cat("\nAll done ✔\n")
