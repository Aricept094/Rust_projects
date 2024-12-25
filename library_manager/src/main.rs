#[derive(Debug)]
struct Book {
    title: String,
    author: String,
    is_borrowed: bool,
    borrow_count: u32,
}

impl Book {
    fn new(title: &str, author: &str) -> Book {
        Book {
            title: title.to_string(),
            author: author.to_string(),
            is_borrowed: false,
            borrow_count: 0,
        }
    }

    fn borrow(&mut self) -> Result<(), String> {
        if self.is_borrowed {
            Err(format!("'{}' by {} is already borrowed", self.title, self.author))
        } else {
            self.is_borrowed = true;
            self.borrow_count += 1;
            Ok(())
        }
    }

    fn return_book(&mut self) -> Result<(), String> {
        if !self.is_borrowed {
            Err(format!("'{}' by {} is not borrowed", self.title, self.author))
        } else {
            self.is_borrowed = false;
            Ok(())
        }
    }

    fn get_stats(&self) -> String {
        format!("'{}' by {} has been borrowed {} times", 
            self.title, 
            self.author, 
            self.borrow_count
        )
    }
}

// Fixed the lifetime parameter here
fn find_books_by_author<'a>(books: &'a Vec<Book>, author: &str) -> Vec<&'a Book> {
    books.iter()
        .filter(|book| book.author.to_lowercase() == author.to_lowercase())
        .collect()
}

fn main() {
    let mut books = vec![
        Book::new("The Rust Programming Language", "Steve Klabnik"),
        Book::new("1984", "George Orwell"),
        Book::new("Animal Farm", "George Orwell"),
        Book::new("The Hobbit", "J.R.R. Tolkien"),
    ];

    println!("Welcome to the Library Manager!\n");

    // Demonstrate book borrowing and returning
    println!("=== Testing Book Operations ===");
    if let Some(book) = books.get_mut(0) {
        println!("Attempting to borrow: {}", book.title);
        match book.borrow() {
            Ok(()) => println!("Successfully borrowed the book"),
            Err(e) => println!("Error: {}", e),
        }
    }

    // Find books by author
    println!("\n=== Finding Books by Author ===");
    let orwell_books = find_books_by_author(&books, "George Orwell");
    println!("Books by George Orwell:");
    for book in orwell_books {
        println!("- {} ({})", book.title, if book.is_borrowed { "borrowed" } else { "available" });
    }

    // Show statistics
    println!("\n=== Book Statistics ===");
    for book in &books {
        println!("{}", book.get_stats());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_book_creation() {
        let book = Book::new("Test Book", "Test Author");
        assert_eq!(book.title, "Test Book");
        assert_eq!(book.author, "Test Author");
        assert_eq!(book.is_borrowed, false);
        assert_eq!(book.borrow_count, 0);
    }

    #[test]
    fn test_book_borrowing() {
        let mut book = Book::new("Test Book", "Test Author");
        assert!(book.borrow().is_ok());
        assert_eq!(book.borrow_count, 1);
        assert!(book.borrow().is_err());
        assert_eq!(book.borrow_count, 1);
    }

    #[test]
    fn test_find_books_by_author() {
        let books = vec![
            Book::new("Book 1", "Author A"),
            Book::new("Book 2", "Author B"),
            Book::new("Book 3", "Author A"),
        ];
        let author_books = find_books_by_author(&books, "Author A");
        assert_eq!(author_books.len(), 2);
        assert_eq!(author_books[0].title, "Book 1");
        assert_eq!(author_books[1].title, "Book 3");
    }
}