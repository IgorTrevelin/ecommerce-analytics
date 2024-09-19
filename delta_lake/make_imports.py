# Databricks notebook source

def checkpoint_location(layer: str, table: str) -> str:
    assert layer in ["bronze", "silver", "gold"]

    return f"/Volumes/dbs_data_eng/default/checkpoints/{layer}/{table}"
