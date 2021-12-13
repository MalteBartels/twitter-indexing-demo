const fs = require("fs");
const readline = require("readline");

/**
 * List Item for a posting
 *
 * @property {number} documentId
 * @property {PostingsListItem} nextPosting
 */
class PostingsListItem {
  constructor(documentId, nextPosting) {
    this.documentId = documentId;
    this.nextPosting = nextPosting;
  }
}

/**
 * Creates an index from a given input file with tweets.
 *
 * @param {string} path - path to the file
 * @returns {[Map, number[]]} - Returns the dictionary and a mapping of line numbers to document ids.
 * Check ids mapping documentation further down in the code to learn more on why there is an ids mapping.
 **/
async function index(path) {
  /** CSV separator token */
  const SEPARATOR = "\t";

  // Create a stream and reader to read the file line by line
  const fileStream = fs.createReadStream(path);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity, // Recognize '\r\n' as single line break
  });

  // Keep track of the line number
  let lineNumber = 0;
  // Keep track of the elapsed time for console output
  let lastI = Date.now();

  /** Resulting dictionary */
  const dictionary = new Map();
  /**
   * Resulting ids mapping: maps document ids to line numbers
   *
   * This is helpful, so that we don't have to sort the incoming
   * documents nor the created postings lists by document id.
   * Line numbers are obviously already sorted.
   */
  const ids = [];

  // Read the file line by line and process it
  for await (const line of rl) {
    // Output the current speed per 100k documents.
    if (lineNumber % 100000 === 0) {
      console.log(`Current speed: ${Date.now() - lastI}ms/100000`);
      lastI = Date.now();
    }

    // Parse the line's data
    const [id, userId, userName, tweetText] = line.split(SEPARATOR);

    // Skip document if the tweet is empty
    if (!tweetText) {
      lineNumber++;
      continue;
    }

    const preprocessed = preprocess(tweetText);
    // Skip document if the tweet is empty after preprocessing
    if (!preprocessed) {
      lineNumber++;
      continue;
    }
    // Create tokens from text
    const tokenized = tokenize(preprocessed);
    // Normalize the tokens
    const normalized = normalize(tokenized);
    // Collect types from the normalized tokens
    const types = collectTypes(normalized);

    // For every type, create or update the corresponding dictionary entry
    types.forEach((type) => {
      // Try to access an existing dictionary item for the type
      const dictionaryItem = dictionary.get(type);

      if (dictionaryItem) {
        // Item exists already. Extend postings list and update size
        const updatedItem = {
          ...dictionaryItem,
          size: dictionaryItem.size + 1,
        };

        // Insert new item to beginning of postings list, because it's much cheaper than appending it
        // (Note that this means our list is sorted decending)
        updatedItem.postingsList = new PostingsListItem(
          lineNumber,
          dictionaryItem.postingsList
        );

        // Update the dictionary
        dictionary.set(type, updatedItem);
      } else {
        // Item doesn't exist yet. Create a new one.
        const newItem = {
          term: type,
          size: 1,
          postingsList: new PostingsListItem(lineNumber, undefined),
        };
        dictionary.set(type, newItem);
      }
    });

    // Write the line number to the ids mapping
    ids[lineNumber] = id;

    // Proceed to next line
    lineNumber++;
  }

  return [dictionary, ids];
}

/**
 * Cleans up a string by replacing unnecessary special characters, newlines and tabs with spaces.
 *
 * Unnecessary characters are all non digit, non letter characters except the hashtag,
 * because people might want to search for hashtags specifically.
 *
 * Note that this is a really basic clean up function.
 *
 * @param {string} text
 * @returns {string} - cleaned up text
 */
function preprocess(text) {
  return text.replace(/\[NEWLINE\]|\[TAB\]|[^\d\w#]/g, " ");
}

/**
 * Creates tokens from a text by splitting on ' '.
 * Empty tokens are ommited.
 *
 * @param {string} text
 * @returns {string[]} - token
 */
function tokenize(text) {
  return text.split(" ").filter((el) => el);
}

/**
 * Applies a set of normalization rules to a list of tokens.
 *
 * @param {string[]} tokens
 * @returns {string[]} normalized tokens
 */
function normalize(tokens) {
  return tokens.map((token) => {
    let normalized = token;

    // Convert to lowercase, even though we might loose information
    // However we expect Twitter users not to care too much about correct usage of cases.
    normalized = normalized.toLowerCase();

    // You can apply more rules here, if you want.

    return normalized;
  });
}

/**
 * Collapses a list of tokens for the same document to
 * a set of unique tokens.
 * Expands hashtags to a token with and without the hastag.
 *
 * @param {string[]} tokens
 * @returns {string[]} types
 */
function collectTypes(tokens) {
  return tokens.reduce((types, token) => {
    // Drop duplicates
    if (types.includes(token)) {
      return types;
    }
    // For hashtags, add hashtag and word without hashtag
    if (token.startsWith("#")) {
      const tokenWithoutHashtag = token.replace(/#/, "");
      // Only add token without hashtag if it's not already in the list
      if (types.includes(tokenWithoutHashtag)) {
        return [...types, token];
      }
      return [...types, token, tokenWithoutHashtag];
    }
    // By default, just add the token to the list
    return [...types, token];
  }, []);
}

/**
 * Returns the result of a query against a given index.
 *
 * Specify the index by providing a dictionary and an id mapping.
 * Specify the query by providing one or more terms.
 * Note that queries are an AND connection of all provided terms.
 *
 * @param {Map} dictionary
 * @param {number[]} idsMap
 * @param  {...string} terms
 * @returns {number[]} Returns an array with all elements that match the query
 */
function query(dictionary, idsMap, ...terms) {
  // If exactly one term is provided, return the postings list.
  if (terms.length === 1) {
    // Iterate through the postings list to create the results array
    const listStart = dictionary.get(terms[0])?.postingsList;
    const result = [];

    let currentItem = listStart;
    while (currentItem) {
      result.push(currentItem.documentId);
      currentItem = currentItem.nextPosting;
    }

    return result.map((result) => idsMap[result]);
  }

  // If two or more terms are provided, intersect all the lists

  // Get all first items of the terms' postings lists
  const allItems = terms.map((term) => dictionary.get(term)?.postingsList);

  // Sort by postings list size, starting with the smallest list
  allItems.sort((a, b) => a.size - b.size);

  // Initialize a new list to store the results
  let resultList = {
    nextPosting: undefined,
  };
  // Keep a pointer to the end of the results list, just because it's helpful
  let currentResultEnd = resultList;

  // Pop the smallest and second smallest postings list starts from all items
  let itemA = allItems.shift();
  let itemB = allItems.shift();

  // Go through all postings lists...
  do {
    // ...always comparing two at a time.
    while (itemA && itemB) {
      if (itemA.documentId === itemB.documentId) {
        // Possible result found. Add it to the results list.
        currentResultEnd.nextPosting = new PostingsListItem(
          itemA.documentId,
          undefined
        );
        // Update list end
        currentResultEnd = currentResultEnd.nextPosting;
        // Move forward through current postings lists
        itemA = itemA.nextPosting;
        itemB = itemB.nextPosting;
      } else if (itemA.documentId > itemB.documentId) {
        // Move list A
        itemA = itemA.nextPosting;
      } else {
        // Move list B
        itemB = itemB.nextPosting;
      }
    }

    // We are done comparing the two lists.

    // Continue with the list of possible results and one new list.
    itemA = resultList.nextPosting;
    itemB = allItems.shift();

    // Check if there is still something to compare
    if (itemA && itemB) {
      // Create a new, empty list of possible results and update the end pointer
      resultList = { nextPosting: undefined };
      currentResultEnd = resultList;
    } else {
      // if there is nothing to compare, stop iterating
      break;
    }
  } while (allItems.length > 0 || (itemA && itemB));

  // Create a result array from the result postings list
  const result = [];
  let posting = resultList.nextPosting;
  while (posting) {
    result.push(posting.documentId);
    posting = posting.nextPosting;
  }

  // Return result, but replace all line numbers with actual document ids
  return result.map((result) => idsMap[result]);
}

// Example execution
const PATH = "twitter.csv";
const onDictionary = index(PATH);

onDictionary.then(([dictionary, ids]) => {
  console.log(`Dictionary with ${dictionary.size} items created successfully`);

  // “show me tweets of people who talk about the side effects of malaria and COVID vaccines”.
  // Query: (side AND effects AND malaria AND vaccine) OR (side AND effects AND covid AND vaccine)
  console.log(
    JSON.stringify(
      query(dictionary, ids, "side", "effect", "malaria", "vaccine")
    )
  );
  console.log(
    JSON.stringify(query(dictionary, ids, "side", "effect", "covid", "vaccine"))
  );
});
