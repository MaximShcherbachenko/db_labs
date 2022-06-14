from flask import Flask, render_template, url_for, request, redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from base64 import b64encode
import os

SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL').replace('postgres', 'postgresql')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Movie(db.Model):
    __tablename__ = 'Movies'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100),  nullable=False)
    intro = db.Column(db.String(300),  nullable=False)
    text = db.Column(db.Text,  nullable=False)
    image = db.Column(db.LargeBinary, nullable=True)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    comments = db.relationship('Comment', backref='movie', lazy=True)

    def __repr__(self):
        return '<Movie %r>' % self.id


class Comment(db.Model):
    __tablename__ = 'Comments'

    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('Movies.id', ondelete='CASCADE'))
    text = db.Column(db.Text, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return '<Comment %r>' % self.id


@app.route('/')
@app.route('/home')
def main():  # put application's code here
    return render_template("index.html")


@app.route('/about')
def about():  # put application's code here
    return render_template("about.html")


@app.route('/create', methods=['POST', 'GET'])
def create():  # put application's code here
    if request.method == "POST":
        title = request.form['title']
        intro = request.form['intro']
        text = request.form['text']
        image = request.files['image']

        movie = Movie(title=title, intro=intro, text=text, image=image.read())

        try:
            db.session.add(movie)
            db.session.commit()

            return redirect('/posts')
        except:
            return "Oops... Smth wrong"
    elif request.method == "GET":
        return render_template("create.html")


@app.route('/posts')
def posts():  # put application's code here
    movies = Movie.query.order_by(Movie.date.desc()).all()
    return render_template("posts.html", movies=movies)


@app.route('/posts/<int:id>', methods=['POST', 'GET'])
def full_post(id):
    if request.method == "POST":
        movie_id = id
        text = request.form['text']

        comment = Comment(movie_id=movie_id, text=text)

        try:
            db.session.add(comment)
            db.session.commit()

            return redirect(request.url)
        except:
            return "Oops... Smth wrong"

    elif request.method == "GET":
        movie = Movie.query.get(id)
        image = b64encode(movie.image).decode('ascii')
        comment = Comment.query.order_by(Comment.date.desc()).all()
        return render_template("full_post.html", movie=movie, image=image, comment=comment)


@app.route('/posts/<int:id>/delete')
def full_post_delete(id):  # put application's code here
    movie = Movie.query.filter_by(id=id).first()

    try:
        db.session.delete(movie)
        db.session.commit()

        return redirect('/posts')
    except:
        return "Error while deleting..."


@app.route('/posts/<int:id>/update', methods=['POST', 'GET'])
def full_post_update(id):
    movie = Movie.query.get(id)
    if request.method == "POST":
        movie.title = request.form['title']
        movie.intro = request.form['intro']
        movie.text = request.form['text']
        image = request.files['image']
        movie.image = image.read()

        try:
            db.session.commit()

            return redirect('/posts')
        except:
            return "Error while updating"
    elif request.method == "GET":
        return render_template("post_update.html", movie=movie)


@app.route('/posts/<int:movie_id>/comments/<int:comment_id>/update', methods=['POST', 'GET'])
def comment_update(movie_id, comment_id):
    comment = Comment.query.get(comment_id)

    if request.method == "POST":
        comment.text = request.form['text']

        try:
            db.session.commit()

            return redirect(f'/posts/{movie_id}')
        except:
            return "Oops... Smth wrong"

    return render_template('comment_update.html', comment=comment)


@app.route('/posts/<int:movie_id>/comments/<int:comment_id>/delete')
def delete_comment(movie_id, comment_id):
    comment = Comment.query.filter_by(id=comment_id).first()

    db.session.delete(comment)
    db.session.commit()

    return redirect(f'/posts/{movie_id}')


if __name__ == '__main__':
    app.run(debug=True)
