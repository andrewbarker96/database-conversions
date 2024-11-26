# SigParser to Supabase Conversion

My company has long struggled with finding a contact solution that fits our needs. Previously our contact system has existed on a "Windows Exchange" Server, though in recent years it has become slow and unreliable. 

Late last year, my company turned to <a href="https://sigparser.com/">"SigParser"</a> as an alternative to better managing our contacts. SigParser is great as it automatically updates contacts based on email signatures. Making it more efficient to keep an updated contact system. However their API costs are quite large for a small business like ourselves so I have looked for ways to create my own conversion method to a reliable Database for our employees can access. 

## End Goal

The end goal of this project is to design an application whether it be desktop, mobile, or both. That puts accurate contact information into the hands of our Engineers & Surveying crews. By developing an application, we eliminate the need for personal contacts to mesh with business contacts by utilizing our own database to pull from. 

## Database Usage

Originally for this project I looked at Firebase to utilize as it is extremely flexible and easy to setup. However due to our contact system being over 20,000 long, I could easily forsee our company going way above the 50,000 max daily limit on Firebase. So we opted to utilize Supabase as its lightweight, easy to deploy, and utilize common PostgreSQL syntax. Plus with its automatic deployment to the cloud and Realtime features, it ensures our employees see the latest Contact information. 