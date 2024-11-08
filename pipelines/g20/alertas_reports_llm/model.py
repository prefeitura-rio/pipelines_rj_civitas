# -*- coding: utf-8 -*-
import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import vertexai
from pydantic import BaseModel
from vertexai.preview import generative_models
from vertexai.preview.generative_models import GenerationConfig, GenerativeModel

SAFETY_CONFIG = {
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_UNSPECIFIED: generative_models.HarmBlockThreshold.BLOCK_NONE,
}


class EnrichResponseModel(BaseModel):
    main_topic: str
    related_topics: List[str]
    scope_level_explanation: str
    scope_level: str
    predicted_time_explanation: str
    predicted_time_interval: str
    threat_explanation: str
    threat_level: str
    title_report: str


class RelationResponseModel(BaseModel):
    relation_explanation: str
    relation_key_factors: List[str]
    relation_confidence: float
    relation: bool
    relation_title: str


class Model:
    def vertex_init(self, project_id: str = "rj-escritorio-dev", location: str = "us-central1"):
        vertexai.init(project=project_id, location=location)

    def vertex_predict(
        self,
        prompt_text: str,
        response_schema: dict,
        model_name: str = "gemini-1.5-flash",
        max_output_tokens: int = 1024,
        temperature: float = 0.2,
        top_k: int = 32,
        top_p: int = 1,
    ):
        model = GenerativeModel(model_name)
        response = model.generate_content(
            prompt_text,
            generation_config=GenerationConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
                max_output_tokens=max_output_tokens,
                temperature=temperature,
                top_k=top_k,
                top_p=top_p,
            ),
            safety_settings=SAFETY_CONFIG,
        )

        for candidate in response.candidates:
            finish_reason = str(candidate.finish_reason)

        if "content" in response.candidates[0].to_dict():
            reponse_dict = json.loads(response.text)
        else:
            reponse_dict = {field: None for field in response_schema.get("properties", {}).keys()}
        reponse_dict["finish_reason"] = finish_reason

        return reponse_dict

    def model_predict_batch(self, model_input: List[dict], retry: int = 5, max_workers: int = 10):
        def process_prompt(model_data, index, total, retry):
            start_time = time.time()
            prompt_text = model_data["prompt_text"]
            response_schema = model_data["response_schema"]

            for attempt in range(retry):
                try:
                    response = self.vertex_predict(
                        prompt_text=prompt_text,
                        response_schema=response_schema,
                        model_name=model_data.get("model_name", "gemini-1.5-flash"),
                        max_output_tokens=model_data.get("max_output_tokens", 1024),
                        temperature=model_data.get("temperature", 0.2),
                        top_k=model_data.get("top_k", 32),
                        top_p=model_data.get("top_p", 1),
                    )
                    print(f"Predicted {index}/{total} in {time.time() - start_time:.2f} seconds")

                    response["index"] = model_data["index"]
                    response["error_name"] = None
                    response["error_message"] = None

                    return response
                except Exception as e:
                    error_name = str(type(e).__name__)
                    error_message = str(traceback.format_exc(chain=False))
                    print(
                        f"Retrying {index}/{total}, retries left {retry - attempt - 1}. Error: {error_name}: {error_message}"
                    )
                    time.sleep(60)

                response = {}
                response["index"] = model_data["index"]
                response["error_name"] = error_name
                response["error_message"] = error_message

                return response

        total_prompts = len(model_input)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_prompt, prompt_data, i + 1, total_prompts, retry)
                for i, prompt_data in enumerate(model_input)
            ]
            results = [future.result() for future in as_completed(futures)]

        return results
