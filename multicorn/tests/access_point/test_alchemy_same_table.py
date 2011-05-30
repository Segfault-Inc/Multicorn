# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

from multicorn.site import Site
from multicorn.item import Item
from multicorn.access_point.alchemy import AlchemyProperty, Alchemy
from nose.tools import eq_

import unittest

from ..common import require

def make_site():
    question_ap = Alchemy("sqlite:///", "question", {
        "id": AlchemyProperty(int),
        "label": AlchemyProperty(unicode),
        "question_group": AlchemyProperty(Item, relation="many-to-one",
            remote_property="id", remote_ap="question_group"),
        "answer": AlchemyProperty(Item, relation="one-to-many",
            remote_property="question", remote_ap="answer")},
        ["id"], True, engine_opts={'echo': True})
    answer_ap = Alchemy("sqlite:///", "answer", {
        "id": AlchemyProperty(int),
        "label": AlchemyProperty(unicode),
        "question": AlchemyProperty(Item, relation="many-to-one",
            remote_property="id", remote_ap="question")},
        ["id"], True, engine_opts={'echo': True})
    question_group_ap = Alchemy("sqlite:///", "questiongroup", {
        "id": AlchemyProperty(int),
        "label": AlchemyProperty(unicode),
        "question": AlchemyProperty(Item, relation="one-to-many",
            remote_property="question_group", remote_ap="question")},
        ["id"], True, engine_opts={'echo': True})
    site = Site()
    site.register("question", question_ap)
    site.register("question_group", question_group_ap)
    site.register("answer", answer_ap)
    return site

def fill_site(site):
    site.create("question_group", {"id": 1, "label": "Group 1"}).save()
    site.create("question", {
        "id": 1,
        "label": "Question 1.1",
        "question_group": 1}).save()
    site.create("question", {
        "id": 2,
        "label": "Question 1.2",
        "question_group": 1}).save()
    site.create("answer", {
        "id": 1,
        "label": "Answer 1.1.1",
        "question": 1}).save()
    site.create("answer", {
        "id": 2,
        "label": "Answer 1.1.2",
        "question": 1}).save()
    site.create("answer", {
        "id": 3,
        "label": "Answer 1.2.1",
        "question": 2}).save()
    site.create("answer", {
        "id": 4,
        "label": "Answer 1.2.2",
        "question": 2}).save()


@require("sqlalchemy")
class TestAlchemy(unittest.TestCase):

    def setUp(self):
        self.site = make_site()
        fill_site(self.site)

    def tearDown(self):
        for access_point in self.site.access_points.values():
            access_point._table.drop()
        Alchemy.__metadatas = {}

    def test_cross_table(self):
        items = list(self.site.view("answer", {
            "question": "question.label",
            "question_group": "question.question_group.label",
            "other_question": "question.question_group.question.label",
            "other_answer": "question.question_group.question.answer.label"
            }))
        # Is it REALLY expected to have 16 items
        eq_(len(items), 16)





